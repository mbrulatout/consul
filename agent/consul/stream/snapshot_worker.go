// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"errors"

	"github.com/hashicorp/go-hclog"
)

// ErrSnapshotFailed reports a recovered snapshot callback panic without exposing its value.
var ErrSnapshotFailed = errors.New("stream snapshot failed")

type snapshotJob struct {
	key            topicSubject
	req            SubscribeRequest
	handler        SnapshotFunc
	snapshot       *eventSnapshot
	anchor         *bufferItem
	buffer         *topicBuffer
	generation     uint64
	waiters        int
	pending        bool
	done           bool
	previous, next *snapshotJob
}

func (e *EventPublisher) snapshotForSubscriptionLocked(req *SubscribeRequest, buffer *topicBuffer, handler SnapshotFunc) (*eventSnapshot, *snapshotJob) {
	if snapshot := e.getCachedSnapshotLocked(req); snapshot != nil {
		return snapshot, nil
	}
	key := req.topicSubject()
	if job := e.snapshotJobs[key]; job != nil {
		job.waiters++
		return job.snapshot, job
	}
	job := &snapshotJob{
		key: key, req: *req, handler: handler,
		snapshot: newEventSnapshot(), anchor: buffer.buf.Head(), buffer: buffer,
		generation: e.generation.Load(), waiters: 1, pending: true,
		previous: e.snapshotTail,
	}
	if e.snapshotTail != nil {
		e.snapshotTail.next = job
	} else {
		e.snapshotHead = job
	}
	e.snapshotTail = job
	e.snapshotJobs[key] = job
	if !e.snapshotWorkerRunning {
		e.snapshotWorkerRunning = true
		go e.runSnapshotJobs()
	}
	return job.snapshot, job
}

func (e *EventPublisher) unlinkSnapshotJobLocked(job *snapshotJob) {
	if !job.pending {
		return
	}
	if job.previous != nil {
		job.previous.next = job.next
	} else {
		e.snapshotHead = job.next
	}
	if job.next != nil {
		job.next.previous = job.previous
	} else {
		e.snapshotTail = job.previous
	}
	job.previous, job.next = nil, nil
	job.pending = false
}

func (e *EventPublisher) finishSnapshotJobLocked(job *snapshotJob) {
	e.unlinkSnapshotJobLocked(job)
	if e.snapshotJobs[job.key] == job {
		delete(e.snapshotJobs, job.key)
	}
	job.done = true
	job.waiters = 0
	// Cleanup closures outlive construction and must not pin snapshot history.
	job.snapshot, job.anchor, job.handler = nil, nil, nil
}

func (e *EventPublisher) retireSnapshotJobLocked(job *snapshotJob, err error) {
	if job.done {
		return
	}
	if job.pending {
		job.snapshot.buffer.AppendItem(&bufferItem{Err: err})
	}
	e.finishSnapshotJobLocked(job)
}

func (e *EventPublisher) releaseSnapshotJobLocked(job *snapshotJob) {
	if job == nil || job.done {
		return
	}
	job.waiters--
	if job.waiters == 0 {
		e.retireSnapshotJobLocked(job, ErrSubForceClosed)
	}
}

func (e *EventPublisher) snapshotJobCurrentLocked(job *snapshotJob) bool {
	return !job.done && job.waiters > 0 && e.snapshotJobs[job.key] == job &&
		e.generation.Load() == job.generation && e.topicBuffers[job.key] == job.buffer
}

func (e *EventPublisher) runSnapshotJobs() {
	for {
		e.lock.Lock()
		job := e.snapshotHead
		if job == nil {
			e.snapshotWorkerRunning = false
			e.lock.Unlock()
			return
		}
		e.unlinkSnapshotJobLocked(job)
		snapshot, anchor, handler, req := job.snapshot, job.anchor, job.handler, job.req
		e.lock.Unlock()

		index, err := runSnapshotHandler(handler, req, snapshot.buffer)

		e.lock.Lock()
		if !e.snapshotJobCurrentLocked(job) {
			snapshot.buffer.AppendItem(&bufferItem{Err: ErrSubForceClosed})
		} else if err != nil {
			snapshot.buffer.AppendItem(&bufferItem{Err: err})
		} else {
			head := job.buffer.buf.Head()
			if len(head.Events) == 0 || head.Events[0].Index <= index {
				anchor = head
			}
			snapshot.complete(req, index, anchor)
			if snapshot.err() == nil {
				e.setCachedSnapshotLocked(&req, snapshot)
			}
		}
		e.finishSnapshotJobLocked(job)
		e.lock.Unlock()
	}
}

func runSnapshotHandler(handler SnapshotFunc, req SubscribeRequest, appender SnapshotAppender) (index uint64, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			hclog.L().Error("panic building stream snapshot", "panic", recovered, "stack", hclog.Stacktrace())
			err = ErrSnapshotFailed
		}
	}()
	return handler(req, appender)
}
