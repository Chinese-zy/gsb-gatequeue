package gatequeue

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Clock supplies the current time as Unix nanoseconds. Tests inject a fake.
type Clock interface{ Now() int64 }

type realClock struct{}

func (realClock) Now() int64 { return nowUnixNano() }

// Entry is one queued item. Seq is assigned at registration and never
// reused: cancelling an entry does not shift later entries' sequences.
type Entry struct {
	ID        string
	Seq       uint64
	ReleaseAt int64 // Unix nanos; releasable when clock.Now() >= ReleaseAt
	Payload   []byte
}

const (
	opAdd       byte = 1
	opTombstone byte = 2 // covers both cancel and released
)

var errClosed = errors.New("gatequeue: queue is closed")

// Queue is a time-gated queue backed by a WAL file.
type Queue struct {
	mu      sync.Mutex
	clock   Clock
	wal     *os.File
	entries map[string]*Entry // live entries by ID
	seqs    map[uint64]string // live entries by Seq
	pending entryHeap         // live entries ordered by (ReleaseAt, Seq)
	nextSeq uint64
	closed  bool
}

// Open loads any fully-registered entries from the WAL at path (discarding
// a torn tail) and returns a ready-to-use queue. A nil clock uses wall time.
func Open(path string, clock Clock) (*Queue, error) {
	if clock == nil {
		clock = realClock{}
	}
	q := &Queue{
		clock:   clock,
		entries: make(map[string]*Entry),
		seqs:    make(map[uint64]string),
		nextSeq: 1,
	}
	if err := q.recover(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	q.wal = f
	return q, nil
}

// Enqueue registers id for release at releaseAt. Re-registering an ID that
// is still queued is a no-op and returns the existing entry with dup=true,
// so the same entry can never be released twice.
func (q *Queue) Enqueue(id string, releaseAt int64, payload []byte) (e Entry, dup bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Entry{}, false, errClosed
	}
	if cur, ok := q.entries[id]; ok {
		return *cur, true, nil
	}
	e = Entry{ID: id, Seq: q.nextSeq, ReleaseAt: releaseAt, Payload: append([]byte(nil), payload...)}
	if err := q.appendRecord(encodeAdd(e)); err != nil {
		return Entry{}, false, err
	}
	q.nextSeq++
	cp := e
	q.entries[id] = &cp
	q.seqs[e.Seq] = id
	heap.Push(&q.pending, &cp)
	return e, false, nil
}

// Cancel removes a queued entry. It returns false if the ID is not queued.
// Sequences of remaining entries are untouched.
func (q *Queue) Cancel(id string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false, errClosed
	}
	e, ok := q.entries[id]
	if !ok {
		return false, nil
	}
	if err := q.appendRecord(encodeTombstone(e.Seq)); err != nil {
		return false, err
	}
	q.removeLocked(e)
	return true, nil
}

// PopReady removes and returns every entry whose release time has arrived
// according to the injected clock, ordered by (ReleaseAt, Seq). Entries not
// yet due stay queued and can never leave early.
func (q *Queue) PopReady() ([]Entry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, errClosed
	}
	now := q.clock.Now()
	var out []Entry
	for q.pending.Len() > 0 && q.pending[0].ReleaseAt <= now {
		e := heap.Pop(&q.pending).(*Entry)
		if err := q.appendRecord(encodeTombstone(e.Seq)); err != nil {
			return out, err
		}
		delete(q.entries, e.ID)
		delete(q.seqs, e.Seq)
		out = append(out, *e)
	}
	return out, nil
}

// Len reports how many entries are still queued.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// SeqOf returns the sequence assigned to a queued ID.
func (q *Queue) SeqOf(id string) (uint64, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	e, ok := q.entries[id]
	if !ok {
		return 0, false
	}
	return e.Seq, true
}

// Close flushes and closes the WAL.
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	return q.wal.Close()
}

func (q *Queue) removeLocked(e *Entry) {
	delete(q.entries, e.ID)
	delete(q.seqs, e.Seq)
	for i, it := range q.pending {
		if it.Seq == e.Seq {
			heap.Remove(&q.pending, i)
			return
		}
	}
}

// Record framing: [uint32 bodyLen][body][uint32 crc32(body)].
// A record is committed only once its full frame hits disk; a crash
// mid-write leaves a torn tail that recovery truncates.

func encodeAdd(e Entry) []byte {
	body := make([]byte, 0, 1+8+8+2+len(e.ID)+len(e.Payload))
	body = append(body, opAdd)
	body = binary.BigEndian.AppendUint64(body, e.Seq)
	body = binary.BigEndian.AppendUint64(body, uint64(e.ReleaseAt))
	body = binary.BigEndian.AppendUint16(body, uint16(len(e.ID)))
	body = append(body, e.ID...)
	body = append(body, e.Payload...)
	return frame(body)
}

func encodeTombstone(seq uint64) []byte {
	body := make([]byte, 0, 9)
	body = append(body, opTombstone)
	body = binary.BigEndian.AppendUint64(body, seq)
	return frame(body)
}

func frame(body []byte) []byte {
	out := make([]byte, 0, 4+len(body)+4)
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	out = append(out, body...)
	out = binary.BigEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return out
}

func (q *Queue) appendRecord(rec []byte) error {
	if _, err := q.wal.Write(rec); err != nil {
		return err
	}
	return q.wal.Sync()
}

// recover replays complete records in file order and truncates the file at
// the first incomplete or corrupt frame, so half-written entries vanish.
func (q *Queue) recover(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var good int64 // offset just past the last fully valid record
	hdr := make([]byte, 4)
	for {
		if _, err := f.ReadAt(hdr, good); err != nil {
			break // short header: torn tail
		}
		n := binary.BigEndian.Uint32(hdr)
		if n < 1 || n > 1<<26 {
			break // implausible length: corrupt tail
		}
		rec := make([]byte, 4+int64(n)+4)
		if _, err := f.ReadAt(rec, good); err != nil {
			break // short body/crc: torn tail
		}
		body := rec[4 : 4+n]
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(rec[4+n:]) {
			break // checksum mismatch: corrupt tail
		}
		if err := q.apply(body); err != nil {
			break // unknown op: treat as corrupt tail
		}
		good += int64(len(rec))
	}
	return f.Truncate(good)
}

func (q *Queue) apply(body []byte) error {
	switch body[0] {
	case opAdd:
		if len(body) < 1+8+8+2 {
			return io.ErrUnexpectedEOF
		}
		seq := binary.BigEndian.Uint64(body[1:9])
		releaseAt := int64(binary.BigEndian.Uint64(body[9:17]))
		idLen := int(binary.BigEndian.Uint16(body[17:19]))
		if len(body) < 19+idLen {
			return io.ErrUnexpectedEOF
		}
		id := string(body[19 : 19+idLen])
		payload := append([]byte(nil), body[19+idLen:]...)
		if _, live := q.entries[id]; live {
			return nil // duplicate registration in log: keep first
		}
		e := &Entry{ID: id, Seq: seq, ReleaseAt: releaseAt, Payload: payload}
		q.entries[id] = e
		q.seqs[seq] = id
		heap.Push(&q.pending, e)
		if seq >= q.nextSeq {
			q.nextSeq = seq + 1
		}
	case opTombstone:
		if len(body) < 9 {
			return io.ErrUnexpectedEOF
		}
		seq := binary.BigEndian.Uint64(body[1:9])
		if id, ok := q.seqs[seq]; ok {
			q.removeLocked(q.entries[id])
		}
	default:
		return fmt.Errorf("gatequeue: unknown wal op %d", body[0])
	}
	return nil
}

// entryHeap orders by earliest ReleaseAt first, ties by Seq.
type entryHeap []*Entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if h[i].ReleaseAt != h[j].ReleaseAt {
		return h[i].ReleaseAt < h[j].ReleaseAt
	}
	return h[i].Seq < h[j].Seq
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)   { *h = append(*h, x.(*Entry)) }
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
