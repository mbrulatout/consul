// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/hashicorp/consul/agent/grpc-internal/resolver"
	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/pool"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
	"github.com/hashicorp/consul/testrpc"
	"github.com/hashicorp/consul/types"
)

func TestStreamingTLSRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real TLS agent")
	}
	baseline := goleak.IgnoreCurrent()
	t.Cleanup(func() {
		goleak.VerifyNone(t, baseline, goleak.IgnoreTopFunction("github.com/hashicorp/consul/sdk/freeport.checkFreedPorts"))
	})
	dir := t.TempDir()
	ca1, cert1, key1 := streamingRotationCertificate(t, 1)
	ca2, cert2, key2 := streamingRotationCertificate(t, 2)
	caFile, certFile, keyFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	write := func(path string, data []byte) { t.Helper(); require.NoError(t, os.WriteFile(path, data, 0600)) }
	write(caFile, ca1)
	write(certFile, cert1)
	write(keyFile, key1)
	a := NewTestAgent(t, fmt.Sprintf(`
 connect { enabled = false }
 peering { enabled = false }
 rpc { enable_streaming = true }
 use_streaming_backend = true
 ca_file = %q
 cert_file = %q
 key_file = %q
 verify_incoming = true
 verify_outgoing = true
 verify_server_hostname = true
 `, caFile, certFile, keyFile))
	testrpc.WaitForLeader(t, a.RPC, "dc1")

	var server *metadata.Server
	for _, member := range a.LANMembersInAgentPartition() {
		if ok, meta := metadata.IsConsulServer(member); ok {
			server = meta
			break
		}
	}
	require.NotNil(t, server)
	rb := a.baseDeps.LeaderForwarder.(*resolver.ServerResolverBuilder)
	conn, err := a.baseDeps.GRPCConnPool.ClientConn("dc1")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := pbsubscribe.NewStateChangeSubscriptionClient(conn)
	subscribe := func() pbsubscribe.StateChangeSubscription_SubscribeClient {
		t.Helper()
		stream, err := client.Subscribe(ctx, &pbsubscribe.SubscribeRequest{Datacenter: "dc1", Topic: pbsubscribe.Topic_ServiceHealth, Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "rotate"}}})
		require.NoError(t, err)
		for {
			event, err := stream.Recv()
			require.NoError(t, err)
			if event.GetEndOfSnapshot() {
				return stream
			}
		}
	}
	register := func(port int) {
		t.Helper()
		var reply struct{}
		require.NoError(t, a.RPC(ctx, "Catalog.Register", &structs.RegisterRequest{Datacenter: "dc1", Node: "rotation-node", Address: "127.0.0.1", Service: &structs.NodeService{ID: "rotate", Service: "rotate", Port: port}}, &reply))
	}
	health := func(port int) {
		t.Helper()
		require.Eventually(t, func() bool {
			req := httptest.NewRequest("GET", "/v1/health/service/rotate?index=1&wait=100ms", nil).WithContext(ctx)
			resp := httptest.NewRecorder()
			result, err := a.srv.HealthServiceNodes(resp, req)
			if err != nil || resp.Header().Get("X-Consul-Query-Backend") != "streaming" {
				return false
			}
			nodes, ok := result.(structs.CheckServiceNodes)
			return ok && len(nodes) == 1 && nodes[0].Service.Port == port
		}, 10*time.Second, 20*time.Millisecond, "streaming health did not converge to port %d", port)
	}
	live := func(stream pbsubscribe.StateChangeSubscription_SubscribeClient, port int) {
		t.Helper()
		register(port)
		var matches func(*pbsubscribe.Event) bool
		matches = func(event *pbsubscribe.Event) bool {
			if update := event.GetServiceHealth(); update != nil && update.CheckServiceNode != nil && update.CheckServiceNode.Service != nil {
				return update.CheckServiceNode.Service.Port == int32(port)
			}
			for _, child := range event.GetEventBatch().GetEvents() {
				if matches(child) {
					return true
				}
			}
			return false
		}
		for {
			event, err := stream.Recv()
			require.NoError(t, err)
			if matches(event) {
				break
			}
		}
		health(port)
	}
	reconnect := func(stream pbsubscribe.StateChangeSubscription_SubscribeClient) {
		t.Helper()
		rb.RemoveServer(types.AreaLAN, server)
		rb.AddServer(types.AreaLAN, server)
		_, err := stream.Recv()
		require.Error(t, err, "removing a server must terminate its established TLS stream")
	}
	probe := func(ca, cert, key []byte) (*tls.ConnectionState, error) {
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		roots := x509.NewCertPool()
		roots.AppendCertsFromPEM(ca)
		raw, err := net.DialTimeout("tcp", server.Addr.String(), time.Second)
		if err != nil {
			return nil, err
		}
		defer raw.Close()
		if err := raw.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return nil, err
		}
		if _, err := raw.Write([]byte{byte(pool.RPCTLS)}); err != nil {
			return nil, err
		}
		secured := tls.Client(raw, &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{pair}, ServerName: "server.dc1.consul", MinVersion: tls.VersionTLS12})
		if err := secured.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		state := secured.ConnectionState()
		return &state, nil
	}
	verifyLeaf := func(ca, cert, key []byte) {
		t.Helper()
		state, err := probe(ca, cert, key)
		require.NoError(t, err)
		block, _ := pem.Decode(cert)
		expected, err := x509.ParseCertificate(block.Bytes)
		require.NoError(t, err)
		require.Equal(t, expected.SerialNumber, state.PeerCertificates[0].SerialNumber)
	}
	reload := func() { t.Helper(); require.NoError(t, a.ReloadConfig()) }

	register(1000)
	stream := subscribe()
	health(1000)
	for cycle := 0; cycle < 3; cycle++ {
		t.Logf("TLS rotation cycle %d: overlap trust, rotate leaf, retire root", cycle)
		write(caFile, append(append([]byte{}, ca1...), ca2...))
		reload()
		live(stream, 1100+cycle*10)
		write(certFile, cert2)
		write(keyFile, key2)
		reload()
		verifyLeaf(ca2, cert2, key2)
		live(stream, 1101+cycle*10)
		write(caFile, ca2)
		reload()
		_, err := probe(ca1, cert1, key1)
		require.Error(t, err, "retired root must not authenticate new connections")
		live(stream, 1102+cycle*10)
		reconnect(stream)
		stream = subscribe()
		live(stream, 1103+cycle*10)
		ca1, ca2 = ca2, ca1
		cert1, cert2 = cert2, cert1
		key1, key2 = key2, key1
	}

	t.Log("invalid replacement retains last loaded TLS credentials")
	write(certFile, cert2)
	require.Error(t, a.ReloadConfig())
	state, err := probe(ca1, cert1, key1)
	require.NoError(t, err)
	expected, _ := pem.Decode(cert1)
	parsed, err := x509.ParseCertificate(expected.Bytes)
	require.NoError(t, err)
	require.Equal(t, parsed.SerialNumber, state.PeerCertificates[0].SerialNumber)
	live(stream, 1200)
	reconnect(stream)
	stream = subscribe()
	live(stream, 1201)
	write(certFile, cert1)
	reload()

	t.Log("new connections fail during trust mismatch and converge after repair")
	write(caFile, ca2)
	reload()
	live(stream, 1300)
	reconnect(stream)
	failedCtx, failedCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	failedStream, err := client.Subscribe(failedCtx, &pbsubscribe.SubscribeRequest{Datacenter: "dc1", Topic: pbsubscribe.Topic_ServiceHealth, Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "rotate"}}})
	if err == nil {
		_, err = failedStream.Recv()
	}
	failedCancel()
	require.Error(t, err, "new stream must fail while the leaf is not trusted")
	write(certFile, cert2)
	write(keyFile, key2)
	reload()
	stream = subscribe()
	live(stream, 1301)
}

func streamingRotationCertificate(t *testing.T, serial int64) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(serial + 100), Subject: pkix.Name{CommonName: fmt.Sprintf("rotation-ca-%d", serial)}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "server.dc1.consul"}, DNSNames: []string{"server.dc1.consul", "localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, caKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
