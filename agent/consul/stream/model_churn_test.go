// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
	"go.uber.org/goleak"
)

type churnVersion struct {
	epoch int
	index uint64
}
type churnPayload struct {
	epoch   int
	key     string
	value   int
	deleted bool
}

func (p churnPayload) Subject() Subject                              { return StringSubject("catalog") }
func (p churnPayload) HasReadPermission(acl.Authorizer) bool         { return true }
func (p churnPayload) ToSubscriptionEvent(uint64) *pbsubscribe.Event { panic("unused") }

type churnGate struct {
	captured, release chan struct{}
	once, releaseOnce sync.Once
}

func newChurnGate() *churnGate {
	return &churnGate{captured: make(chan struct{}), release: make(chan struct{})}
}
func (g *churnGate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

type churnModel struct {
	mu         sync.Mutex
	nextCommit uint64
	version    churnVersion
	state      map[string]int
	history    map[churnVersion]map[string]int
	previous   map[churnVersion]churnVersion
	txSizes    map[churnVersion]int
	gate       *churnGate
	trace      []string
}

func (m *churnModel) current() churnVersion { m.mu.Lock(); defer m.mu.Unlock(); return m.version }
func (m *churnModel) verify(v churnVersion, got map[string]int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	want, ok := m.history[v]
	if !ok {
		return fmt.Errorf("unknown version %+v", v)
	}
	if !maps.Equal(want, got) {
		return fmt.Errorf("version %+v state mismatch: want=%v got=%v", v, want, got)
	}
	return nil
}
func (m *churnModel) snapshot(ctx context.Context, req SubscribeRequest, out SnapshotAppender) (uint64, error) {
	m.mu.Lock()
	v, state, gate := m.version, maps.Clone(m.state), m.gate
	m.mu.Unlock()
	if gate != nil {
		gate.once.Do(func() { close(gate.captured) })
		select {
		case <-gate.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	keys := slices.Sorted(maps.Keys(state))
	for _, key := range keys {
		out.Append([]Event{{Topic: req.Topic, Index: v.index, Payload: churnPayload{epoch: v.epoch, key: key, value: state[key]}}})
		runtime.Gosched()
	}
	return v.index, nil
}
func (m *churnModel) commit(p *EventPublisher, topic Topic, rng *rand.Rand) {
	m.mu.Lock()
	previous := m.version
	m.nextCommit++
	m.version.index = m.nextCommit
	v := m.version
	keys := rng.Perm(12)[:1+rng.Intn(4)]
	batch := make([]Event, 0, len(keys)+1)
	for _, n := range keys {
		key := fmt.Sprintf("service-%02d", n)
		deleted := rng.Intn(4) == 0
		value := v.epoch*1000000 + int(v.index)*100 + n
		if deleted {
			delete(m.state, key)
		} else {
			m.state[key] = value
		}
		batch = append(batch, Event{Topic: topic, Index: v.index, Payload: churnPayload{epoch: v.epoch, key: key, value: value, deleted: deleted}})
	}
	m.state["_transaction"] = int(v.index)
	batch = append(batch, Event{Topic: topic, Index: v.index, Payload: churnPayload{epoch: v.epoch, key: "_transaction", value: int(v.index)}})
	m.history[v] = maps.Clone(m.state)
	m.txSizes[v] = len(batch)
	m.previous[v] = previous
	m.trace = append(m.trace, fmt.Sprintf("commit epoch=%d index=%d members=%d", v.epoch, v.index, len(batch)))
	m.mu.Unlock()
	p.Publish(batch)
	if rng.Intn(3) == 0 {
		runtime.Gosched()
	}
}
func churnRetention(p *EventPublisher, topic Topic, window int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	b := p.bufferForSubscription(topicSubject{Topic: topic.String(), Subject: StringSubject("catalog").String()})
	b.buf.retention = window
}

type churnConsumer struct {
	pub                        *EventPublisher
	model                      *churnModel
	topic                      Topic
	window                     int
	sub                        *Subscription
	version, pendingVersion    churnVersion
	complete                   bool
	state, pending             map[string]int
	resets, snapshots, batches int
}

func (c *churnConsumer) disconnect() {
	if c.sub != nil {
		c.sub.Unsubscribe()
		c.sub = nil
	}
}
func (c *churnConsumer) reset() {
	c.disconnect()
	c.complete = false
	c.version = churnVersion{}
	c.pendingVersion = churnVersion{}
	c.state = nil
	c.pending = map[string]int{}
	c.resets++
}
func (c *churnConsumer) connect() error {
	churnRetention(c.pub, c.topic, c.window)
	index := uint64(0)
	if c.complete {
		index = c.version.index
	} else {
		c.pending = map[string]int{}
		c.pendingVersion = churnVersion{}
	}
	sub, err := c.pub.Subscribe(&SubscribeRequest{Topic: c.topic, Subject: StringSubject("catalog"), Index: index})
	c.sub = sub
	return err
}
func (c *churnConsumer) consume(event Event) error {
	if event.IsNewSnapshotToFollow() {
		c.complete = false
		c.pending = map[string]int{}
		c.pendingVersion = churnVersion{}
		return nil
	}
	if event.IsEndOfSnapshot() {
		if c.pendingVersion.index != event.Index {
			return fmt.Errorf("partial snapshot: pending=%+v end=%d", c.pendingVersion, event.Index)
		}
		c.state = c.pending
		c.version = c.pendingVersion
		c.complete = true
		c.snapshots++
		return c.model.verify(c.version, c.state)
	}
	events := []Event{event}
	if batch, ok := event.Payload.(*PayloadEvents); ok {
		events = batch.Items
	}
	if len(events) == 0 {
		return fmt.Errorf("empty data batch")
	}
	first, ok := events[0].Payload.(churnPayload)
	if !ok {
		return fmt.Errorf("unexpected payload %T", events[0].Payload)
	}
	v := churnVersion{first.epoch, event.Index}
	if c.complete {
		if v.epoch != c.version.epoch {
			return fmt.Errorf("stale generation crossed live boundary: current=%+v event=%+v", c.version, v)
		}
		if v.index == c.version.index {
			return nil
		}
		if v.index < c.version.index {
			return fmt.Errorf("out-of-order live transaction current=%+v event=%+v", c.version, v)
		}
		c.model.mu.Lock()
		size := c.model.txSizes[v]
		previous := c.model.previous[v]
		c.model.mu.Unlock()
		if previous != c.version {
			return fmt.Errorf("missing transaction before %+v: expected predecessor=%+v actual=%+v", v, previous, c.version)
		}
		if len(events) != size {
			return fmt.Errorf("partial transaction %+v: want %d members got %d", v, size, len(events))
		}
	} else if c.pendingVersion.index != 0 && c.pendingVersion != v {
		return fmt.Errorf("mixed snapshot versions %+v %+v", c.pendingVersion, v)
	}
	target := c.state
	if !c.complete {
		target = c.pending
		c.pendingVersion = v
	}
	for _, item := range events {
		payload, ok := item.Payload.(churnPayload)
		if !ok || payload.epoch != v.epoch || item.Index != v.index {
			return fmt.Errorf("incoherent transaction %+v: %+v", v, item)
		}
		if payload.deleted {
			delete(target, payload.key)
		} else {
			target[payload.key] = payload.value
		}
	}
	if c.complete {
		c.version = v
		c.batches++
		return c.model.verify(v, c.state)
	}
	return nil
}
func (c *churnConsumer) catchUp(ctx context.Context, target churnVersion) error {
	for reads := 0; reads < 4096; reads++ {
		if c.complete && (c.version.epoch > target.epoch || c.version.epoch == target.epoch && c.version.index >= target.index) {
			return nil
		}
		if c.sub == nil {
			if err := c.connect(); err != nil {
				return err
			}
		}
		event, err := c.sub.Next(ctx)
		if errors.Is(err, ErrSubForceClosed) {
			c.reset()
			continue
		}
		if err != nil {
			return fmt.Errorf("catchup target=%+v current=%+v reads=%d: %w", target, c.version, reads, err)
		}
		if err := c.consume(event); err != nil {
			return err
		}
	}
	return fmt.Errorf("failed to converge after 4096 records, target=%+v current=%+v", target, c.version)
}

func TestEventPublisher_ModelChurn(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	seeds := make([]int64, 29, 36)
	for i := range seeds {
		seeds[i] = int64(i)
	}
	seeds = append(seeds, 42, 53, 127, 31415, 73425, 99991, 1792849973797633879)
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) { runChurnModel(t, seed) })
	}
}
func runChurnModel(t *testing.T, seed int64) {
	workCtx, stopWork := context.WithTimeout(context.Background(), 30*time.Second)
	pubCtx, stopPub := context.WithCancel(context.Background())
	publisher := NewEventPublisher(time.Minute)
	topic := StringTopic("model-churn")
	model := &churnModel{
		nextCommit: 10,
		version:    churnVersion{1, 10},
		state:      map[string]int{"_generation": 1, "_transaction": 10},
		history:    make(map[churnVersion]map[string]int),
		txSizes:    make(map[churnVersion]int),
		previous:   make(map[churnVersion]churnVersion),
	}
	model.history[model.version] = maps.Clone(model.state)
	if err := publisher.RegisterHandler(topic, func(req SubscribeRequest, out SnapshotAppender) (uint64, error) {
		return model.snapshot(workCtx, req, out)
	}, false); err != nil {
		t.Fatal(err)
	}
	pubDone := make(chan struct{})
	go func() { defer close(pubDone); publisher.Run(pubCtx) }()
	consumers := make([]*churnConsumer, 3)
	for i := range consumers {
		consumers[i] = &churnConsumer{
			pub: publisher, model: model, topic: topic,
			window: 4 + int(uint64(seed)%29), pending: map[string]int{},
		}
	}
	var actors sync.WaitGroup
	defer func() {
		stopWork()
		model.mu.Lock()
		g := model.gate
		model.mu.Unlock()
		if g != nil {
			g.open()
		}
		joined := make(chan struct{})
		go func() { actors.Wait(); close(joined) }()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("actor cleanup did not terminate")
		}
		for _, c := range consumers {
			c.disconnect()
		}
		stopPub()
		select {
		case <-pubDone:
		case <-time.After(5 * time.Second):
			t.Error("publisher cleanup did not terminate")
		}
		if t.Failed() {
			model.mu.Lock()
			t.Logf("seed=%d trace:\n%s", seed, strings.Join(model.trace, "\n"))
			model.mu.Unlock()
		}
	}()
	rng := rand.New(rand.NewSource(seed))
	for round := 0; round < 8; round++ {
		// Hold a coherent snapshot while live publication crosses retention.
		gate := newChurnGate()
		model.mu.Lock()
		model.gate = gate
		model.mu.Unlock()
		if err := publisher.RefreshTopic(topic); err != nil {
			t.Fatal(err)
		}
		consumers[0].reset()
		if err := consumers[0].connect(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-gate.captured:
		case <-workCtx.Done():
			t.Fatal("snapshot capture barrier timed out")
		}
		errorsCh := make(chan error, 3)
		producerDone := make(chan struct{})
		checkpoint := make(chan struct{})
		continueProducer := make(chan struct{})
		ready := make(chan struct{})
		actorSeed := rng.Int63()
		doRestore := rng.Intn(2) == 0
		refreshAt := rng.Intn(13) + 12
		actors.Add(1)
		go func() {
			defer actors.Done()
			defer close(producerDone)
			local := rand.New(rand.NewSource(actorSeed))
			for step := 0; step < 36; step++ {
				if workCtx.Err() != nil {
					return
				}
				if step == refreshAt {
					if doRestore {
						model.mu.Lock()
						// Restoring older catalog state must not rewind future Raft indexes.
						model.version = churnVersion{model.version.epoch + 1, uint64(10 + local.Intn(3))}
						model.state = map[string]int{"_generation": model.version.epoch, "_transaction": int(model.version.index)}
						model.history[model.version] = maps.Clone(model.state)
						model.trace = append(model.trace, fmt.Sprintf("restore %+v", model.version))
						model.mu.Unlock()
						publisher.RefreshAllTopics()
						churnRetention(publisher, topic, consumers[0].window)
					} else {
						if err := publisher.RefreshTopic(topic); err != nil {
							errorsCh <- err
							return
						}
						model.mu.Lock()
						model.trace = append(model.trace, "refresh topic")
						model.mu.Unlock()
					}
				}
				model.commit(publisher, topic, local)
				if step == 8 {
					close(checkpoint)
					select {
					case <-continueProducer:
					case <-workCtx.Done():
						return
					}
				}
			}
		}()
		for n := 0; n < 2; n++ {
			c := consumers[n]
			localSeed := rng.Int63()
			actors.Add(1)
			go func() {
				defer actors.Done()
				local := rand.New(rand.NewSource(localSeed))
				for op := 0; ; op++ {
					if n == 1 && local.Intn(3) == 0 {
						c.reset()
					}
					ctx, cancel := context.WithTimeout(workCtx, 5*time.Second)
					err := c.catchUp(ctx, model.current())
					cancel()
					if err != nil {
						errorsCh <- err
						return
					}
					if n == 0 && op == 0 {
						close(ready)
					}
					select {
					case <-producerDone:
						return
					default:
					}
					runtime.Gosched()
				}
			}()
		}
		select {
		case <-checkpoint:
		case <-workCtx.Done():
			t.Fatal("producer checkpoint stalled")
		}
		gate.open()
		select {
		case <-ready:
		case err := <-errorsCh:
			t.Fatalf("consumer readiness: %v", err)
		case <-workCtx.Done():
			t.Fatal("consumer readiness stalled")
		}
		close(continueProducer)
		select {
		case <-producerDone:
		case <-workCtx.Done():
			t.Fatal("serialized producer stalled")
		}
		gate.open()
		actors.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatalf("round=%d: %v", round, err)
			}
		}
		model.mu.Lock()
		model.gate = nil
		target := model.version
		want := maps.Clone(model.state)
		model.mu.Unlock()
		// The third consumer stays idle until the publication burst finishes.
		for n, c := range consumers {
			if n == 2 && rng.Intn(2) == 0 {
				c.reset()
			}
			ctx, cancel := context.WithTimeout(workCtx, 5*time.Second)
			err := c.catchUp(ctx, target)
			cancel()
			if err != nil {
				t.Fatalf("round=%d consumer=%d: %v", round, n, err)
			}
			if c.version != target || !maps.Equal(c.state, want) {
				t.Fatalf("quiescent barrier round=%d consumer=%d target=%+v got=%+v want=%v gotmap=%v", round, n, target, c.version, want, c.state)
			}
		}
	}
	totals := [3]int{}
	for _, c := range consumers {
		totals[0] += c.resets
		totals[1] += c.snapshots
		totals[2] += c.batches
	}
	if totals[2] == 0 {
		t.Fatal("coverage gap: no live transaction batches observed")
	}
	t.Logf("seed=%d retention=%d commits=288 resets=%d snapshots=%d live_batches=%d final=%+v", seed, consumers[0].window, totals[0], totals[1], totals[2], model.current())
}
