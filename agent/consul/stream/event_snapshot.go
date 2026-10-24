// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

// eventSnapshot represents the state of memdb for a given topic and key at some
// point in time. It is modelled as a buffer of events so that snapshots can be
// streamed to possibly multiple subscribers concurrently, and can be trivially
// cached by retaining a reference to a Snapshot. Once the reference to eventSnapshot
// is dropped from memory, any subscribers still reading from it may do so by following
// their pointers. When the last subscriber unsubscribes, the snapshot is garbage
// collected automatically by Go's runtime. This simplifies snapshot and buffer
// management dramatically.
type eventSnapshot struct {
	// First item in the buffer. Used as the Head of a subscription, or to
	// splice this snapshot onto another one.
	First *bufferItem

	// buffer is the Head of the snapshot buffer the fn should write to.
	buffer *eventBuffer

	liveStart *bufferLink
	cacheID   uint64
}

// newEventSnapshot creates an empty snapshot buffer.
func newEventSnapshot() *eventSnapshot {
	snapBuffer := newEventBuffer()
	return &eventSnapshot{
		First:  snapBuffer.Head(),
		buffer: snapBuffer,
	}
}

// appendAndSlice populates the snapshot buffer by calling the SnapshotFunc,
// then adding an endOfSnapshot framing event, and finally by splicing in
// events from the topicBuffer.
func (s *eventSnapshot) appendAndSplice(req SubscribeRequest, fn SnapshotFunc, topicBufferHead *bufferItem) {
	idx, err := fn(req, s.buffer)
	if err != nil {
		s.buffer.AppendItem(&bufferItem{Err: err})
		return
	}
	s.complete(req, idx, topicBufferHead)
}

func (s *eventSnapshot) complete(req SubscribeRequest, idx uint64, topicBufferHead *bufferItem) {
	// If the SnapshotFunc returned a zero index but no error, it is likely because
	// the resource doesn't exist yet. We don't want to surface this to subscribers
	// though, because some (e.g. submatview.Materializer) assume zero isn't valid
	// and will block/wait to get a higher index.
	//
	// Instead, we return 1, which is safe because (due to Raft's boostrapping) it
	// will never conflict with real user data.
	if idx == 0 {
		idx = 1
	}
	s.buffer.Append([]Event{{
		Topic:   req.Topic,
		Index:   idx,
		Payload: endOfSnapshot{},
	}})
	s.spliceFromTopicBuffer(topicBufferHead, idx)
}

// spliceFromTopicBuffer traverses the topicBuffer looking for the last item
// in the buffer, or the first item where the index is greater than idx. Once
// the item is found it is appended to the snapshot buffer.
func (s *eventSnapshot) spliceFromTopicBuffer(topicBufferHead *bufferItem, idx uint64) {
	item := topicBufferHead
	for {
		switch {
		case item.Err != nil:
			s.buffer.AppendItem(item)
			return

		case len(item.Events) > 0 && item.Events[0].Index > idx:
			if next, ok := item.NextNoBlock(); ok && next.Err != nil {
				s.buffer.AppendItem(next)
				return
			}
			// We've found an update in the topic buffer that happened after our
			// snapshot was taken, splice it into the snapshot buffer so subscribers
			// can continue to read this and others after it.
			s.liveStart = item.link
			s.buffer.AppendItem(item)
			return
		}

		next, ok := item.NextNoBlock()
		if !ok {
			// We reached the head of the topic buffer. We don't want any of the
			// events in the topic buffer as they came before the snapshot.
			// Append a link to any future items.
			s.liveStart = next.link
			s.buffer.AppendItem(next)
			return
		}
		// Proceed to the next item in the topic buffer
		item = next
	}
}

// err returns an error if the snapshot func has failed with an error or nil
// otherwise. Nil doesn't necessarily mean there won't be an error but there
// hasn't been one yet.
func (s *eventSnapshot) err() error {
	// Fetch the head of the buffer, this is atomic. If the snapshot func errored
	// then the last event will be an error.
	head := s.buffer.Head()
	return head.Err
}
