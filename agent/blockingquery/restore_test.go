// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package blockingquery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/structs"
)

func TestQuery_RereadsRestoredStore(t *testing.T) {
	for _, tc := range []struct {
		name               string
		missing            bool
		before, after      error
		oldValue, newValue string
	}{
		{name: "changed result", oldValue: "old", newValue: "restored"},
		{name: "missing result", missing: true, before: ErrNotFound, after: ErrNotFound},
		{name: "unchanged result", after: ErrNotChanged, oldValue: "same", newValue: "same"},
	} {
		for _, minIndex := range []uint64{99, 100} {
			t.Run(fmt.Sprintf("%s/minimum=%d", tc.name, minIndex), func(t *testing.T) {
				old := state.NewStateStore(nil)
				replacement := state.NewStateStore(nil)
				key := "watched"
				if tc.missing {
					key = "other"
				}
				require.NoError(t, old.KVSSet(100, &structs.DirEntry{Key: key, Value: []byte(tc.oldValue)}))
				require.NoError(t, replacement.KVSSet(50, &structs.DirEntry{Key: key, Value: []byte(tc.newValue)}))

				shutdown := make(chan struct{})
				var stopOnce sync.Once
				stop := func() { stopOnce.Do(func() { close(shutdown) }) }
				defer stop()
				current := old
				fsm := restoreTestFSM(t, func() *state.Store { return current }, shutdown)
				meta := &structs.QueryMeta{}
				var result *structs.DirEntry
				calls := 0
				query := func(ws memdb.WatchSet, store *state.Store) error {
					calls++
					index, entry, err := store.KVSGet(ws, "watched", nil)
					if err != nil {
						return err
					}
					meta.Index, result = index, entry
					if store == old {
						current = replacement
						old.Abandon()
						return tc.before
					}
					return tc.after
				}
				done := make(chan error, 1)
				go func() {
					done <- Query(fsm, &structs.QueryOptions{MinQueryIndex: minIndex}, meta, query)
				}()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(time.Second):
					stop()
					<-done
					t.Fatal("query blocked instead of returning the restored store")
				}
				require.Equal(t, uint64(50), meta.Index)
				require.Equal(t, 2, calls)
				if tc.missing {
					require.Nil(t, result)
				} else {
					require.Equal(t, tc.newValue, string(result.Value))
				}
			})
		}
	}
}

func TestQuery_ShutdownStopsWait(t *testing.T) {
	store := state.NewStateStore(nil)
	require.NoError(t, store.KVSSet(100, &structs.DirEntry{Key: "watched", Value: []byte("value")}))
	shutdown := make(chan struct{})
	fsm := restoreTestFSM(t, func() *state.Store { return store }, shutdown)
	queried := make(chan struct{})
	calls := 0
	done := make(chan error, 1)
	go func() {
		meta := &structs.QueryMeta{}
		done <- Query(fsm, &structs.QueryOptions{MinQueryIndex: 100}, meta, func(ws memdb.WatchSet, store *state.Store) error {
			calls++
			index, _, err := store.KVSGet(ws, "watched", nil)
			meta.Index = index
			if calls == 1 {
				close(queried)
			}
			return err
		})
	}()
	<-queried
	close(shutdown)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not interrupt the blocking query")
	}
	require.Equal(t, 1, calls)
}

func restoreTestFSM(t *testing.T, store func() *state.Store, shutdown chan struct{}) *MockFSMServer {
	t.Helper()
	fsm := NewMockFSMServer(t)
	fsm.On("GetShutdownChannel").Return(shutdown)
	fsm.On("GetState").Return(store)
	fsm.On("SetQueryMeta", mock.Anything, mock.Anything).Return()
	fsm.On("RPCQueryTimeout", mock.Anything).Return(time.Hour)
	fsm.On("IncrementBlockingQueries").Return(uint64(1))
	fsm.On("DecrementBlockingQueries").Return(uint64(0))
	return fsm
}

func TestQuery_ShutdownStopsAbandonedRetry(t *testing.T) {
	abandoned := state.NewStateStore(nil)
	abandoned.Abandon()
	live := state.NewStateStore(nil)
	shutdown := make(chan struct{})
	close(shutdown)
	calls := 0
	fsm := restoreTestFSM(t, func() *state.Store {
		if calls == 0 {
			return abandoned
		}
		return live
	}, shutdown)
	meta := &structs.QueryMeta{}
	err := Query(fsm, &structs.QueryOptions{MinQueryIndex: 99}, meta, func(_ memdb.WatchSet, _ *state.Store) error {
		calls++
		meta.Index = 100
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls, "shutdown must stop retries of an abandoned store")
}
