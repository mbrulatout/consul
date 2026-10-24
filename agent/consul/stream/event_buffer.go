// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"weak"
)

// eventBuffer is a single-writer, multiple-reader linked buffer. Snapshots use
// unlimited buffers so every snapshot event survives until delivery. Live topic
// buffers bound their retained transaction history; readers that fall behind
// must reset their view and subscribe again.
//
// Appends must be externally synchronized. Readers follow atomic next pointers
// and wait on immutable channels without taking the publisher lock.
type eventBuffer struct {
	head atomic.Value

	retention int

	// Weak slots let healthy streams release payloads before the window fills.
	retainedLinks []weak.Pointer[bufferLink]
	nextLink      int
}

var expiredBufferItem = &bufferItem{Err: ErrSubForceClosed}

// newEventBuffer creates an eventBuffer ready for use.
func newEventBuffer() *eventBuffer {
	b := &eventBuffer{}
	b.head.Store(newBufferItem(nil))
	return b
}

// Append a set of events from one raft operation to the buffer and notify
// watchers. Note that events must not have been previously made available to
// any other goroutine since we may mutate them to ensure ACL Rules are
// populated. After calling append, the caller must not make any further
// mutations to the events as they may have been exposed to subscribers in other
// goroutines. Append only supports a single concurrent caller and must be
// externally synchronized with other Append, AppendBuffer or AppendErr calls.
func (b *eventBuffer) Append(events []Event) {
	b.AppendItem(newBufferItem(events))
}

// AppendItem joins another buffer which may be the tail of a separate buffer
// for example a buffer that's had the events from a snapshot appended may
// finally by linked to the topic buffer for the subsequent events so
// subscribers can seamlessly consume the updates. Note that Events in item must
// already be fully populated with ACL rules and must not be mutated further as
// they may have already been published to subscribers.
//
// AppendBuffer only supports a single concurrent caller and must be externally
// synchronized with other Append, AppendBuffer or AppendErr calls.
func (b *eventBuffer) AppendItem(item *bufferItem) *bufferLink {
	// First store it as the next node for the old head this ensures once it's
	// visible to new searchers the linked list is already valid. Not sure it
	// matters but this seems nicer.
	oldHead := b.Head()
	oldHead.link.next.Store(item)
	b.head.Store(item)

	// Now it's added invalidate the oldHead to notify waiters
	close(oldHead.link.ch)
	// don't set chan to nil since that will race with readers accessing it.
	if b.retention == 0 {
		return nil
	}
	if len(b.retainedLinks) < b.retention {
		if len(b.retainedLinks) == cap(b.retainedLinks) {
			links := make([]weak.Pointer[bufferLink], len(b.retainedLinks), min(max(1, 2*len(b.retainedLinks)), b.retention))
			copy(links, b.retainedLinks)
			b.retainedLinks = links
		}
		b.retainedLinks = append(b.retainedLinks, weak.Make(oldHead.link))
		return nil
	}

	expired := b.retainedLinks[b.nextLink].Value()
	if expired != nil {
		// Closing a subscription alone leaves blocked readers retaining the suffix.
		expired.next.Store(expiredBufferItem)
	}
	b.retainedLinks[b.nextLink] = weak.Make(oldHead.link)
	b.nextLink = (b.nextLink + 1) % len(b.retainedLinks)
	return expired
}

// Head returns the current head of the buffer. It will always exist but it may
// be a "sentinel" empty item with a nil Events slice to allow consumers to
// watch for the next update. Consumers should always check for empty Events and
// treat them as no-ops. Will panic if eventBuffer was not initialized correctly
// with eventBuffer.
func (b *eventBuffer) Head() *bufferItem {
	return b.head.Load().(*bufferItem)
}

// bufferItem represents a set of events published by a single raft operation.
// The first item returned by a newly constructed buffer will have nil Events.
// It is a sentinel value which is used to wait on the next events via Next.
//
// To iterate to the next event, a Next method may be called which may block if
// there is no next element yet.
//
// Holding a pointer keeps subsequent events until their links are truncated
// so it's important that subscribers don't hold pointers to buffer items after
// they have been delivered except where it's intentional to maintain a cache or
// trailing store of events for performance reasons.
//
// Subscribers must not mutate the bufferItem or the Events or Encoded payloads
// inside as these are shared between all readers.
type bufferItem struct {
	// Events is the set of events published at one raft index. This may be nil as
	// a sentinel value to allow watching for the first event in a buffer. Callers
	// should check and skip nil Events at any point in the buffer. It will also
	// be nil if the producer appends an Error event because they can't complete
	// the request to populate the buffer. Err will be non-nil in this case.
	Events []Event

	// Err is non-nil if the producer can't complete their task and terminates the
	// buffer. Subscribers should return the error to clients and cease attempting
	// to read from the buffer.
	Err error

	// link holds the next pointer and channel. This extra bit of indirection
	// allows us to splice buffers together at arbitrary points without including
	// events in one buffer just for the side-effect of watching for the next set.
	// The link may not be mutated once the event is appended to a buffer.
	link *bufferLink
}

type bufferLink struct {
	// next is set before ch closes. Bounded buffers may replace it with a
	// terminal error when the reader falls outside the retention window.
	next atomic.Value

	// ch is closed when the next event is published. It should never be mutated
	// (e.g. set to nil) as that is racey, but is closed once when the next event
	// is published. the next pointer will have been set by the time this is
	// closed.
	ch chan struct{}
}

// newBufferItem returns a blank buffer item with a link and chan ready to have
// the fields set and be appended to a buffer.
func newBufferItem(events []Event) *bufferItem {
	return &bufferItem{
		link:   &bufferLink{ch: make(chan struct{})},
		Events: events,
	}
}

// Next return the next buffer item in the buffer. It may block until ctx is
// cancelled or until the next item is published.
func (i *bufferItem) Next(ctx context.Context, closed <-chan struct{}) (*bufferItem, error) {
	// See if there is already a next value, block if so. Note we don't rely on
	// state change (chan nil) as that's not threadsafe but detecting close is.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, fmt.Errorf("subscription closed")
	case <-i.link.ch:
	}

	// If channel closed, there must be a next item to read
	nextRaw := i.link.next.Load()
	if nextRaw == nil {
		// shouldn't be possible
		return nil, errors.New("invalid next item")
	}
	next := nextRaw.(*bufferItem)
	if next.Err != nil {
		return nil, next.Err
	}
	return next, nil
}

// NextNoBlock returns true and the next item in the buffer without blocking.
// If there are no subsequent items it returns false and an empty item.
// The empty item will be ignored by subscribers, but has the same link as this
// bufferItem without the Events.
// When the link.ch of the empty item is closed, subscriptions are notified of
// the next item.
func (i *bufferItem) NextNoBlock() (*bufferItem, bool) {
	nextRaw := i.link.next.Load()
	if nextRaw == nil {
		// Return an empty item that can be followed to the next item published.
		return &bufferItem{link: i.link}, false
	}
	return nextRaw.(*bufferItem), true
}

// HasEventIndex returns true if index matches the Event.Index of this item. Returns
// false if there are no events stored in the item, or the index does not match.
func (i *bufferItem) HasEventIndex(index uint64) bool {
	return len(i.Events) > 0 && i.Events[0].Index == index
}
