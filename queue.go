package gatequeue

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrDuplicate   = errors.New("gatequeue: duplicate registration")
	ErrNotFound    = errors.New("gatequeue: entry not found")
	ErrCanceled    = errors.New("gatequeue: entry is canceled")
	ErrReleased    = errors.New("gatequeue: entry is already released")
	ErrCorruptLog  = errors.New("gatequeue: corrupt queue log")
	ErrQueueClosed = errors.New("gatequeue: queue is closed")
)

const (
	recordHeaderSize = 4
	recordCheckSize  = 8
	maxRecordPayload = 1 << 20
)

type Clock interface {
	Now() int64
}

type systemClock struct{}

func (systemClock) Now() int64 { return time.Now().UnixNano() }

type Ticket struct {
	Seq     uint64
	ID      string
	ReadyAt int64
}

type status int

const (
	statusActive status = iota
	statusCanceled
	statusReleased
)

type entry struct {
	ticket Ticket
	status status
}

type recordType string

const (
	recordRegister recordType = "register"
	recordCancel   recordType = "cancel"
	recordRelease  recordType = "release"
)

type record struct {
	Type    recordType `json:"type"`
	Seq     uint64     `json:"seq"`
	ID      string     `json:"id,omitempty"`
	ReadyAt int64      `json:"ready_at,omitempty"`
}

type Queue struct {
	mu      sync.Mutex
	file    *os.File
	clock   Clock
	entries map[string]*entry
	bySeq   map[uint64]*entry
	nextSeq uint64
	closed  bool
}

func New(path string, clock Clock) (*Queue, error) {
	if clock == nil {
		clock = systemClock{}
	}

	q := &Queue{
		clock:   clock,
		entries: make(map[string]*entry),
		bySeq:   make(map[uint64]*entry),
	}

	if path == "" {
		return q, nil
	}

	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	q.file = f

	if err := q.recover(); err != nil {
		_ = f.Close()
		return nil, err
	}

	return q, nil
}

func (q *Queue) Register(id string, readyAt int64) (Ticket, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if id == "" {
		return Ticket{}, errors.New("gatequeue: id is required")
	}
	if q.closed {
		return Ticket{}, ErrQueueClosed
	}
	if _, exists := q.entries[id]; exists {
		return Ticket{}, ErrDuplicate
	}

	ticket := Ticket{
		Seq:     q.nextSeq + 1,
		ID:      id,
		ReadyAt: readyAt,
	}
	rec := record{
		Type:    recordRegister,
		Seq:     ticket.Seq,
		ID:      ticket.ID,
		ReadyAt: ticket.ReadyAt,
	}
	if err := q.append(rec); err != nil {
		return Ticket{}, err
	}

	item := &entry{ticket: ticket, status: statusActive}
	q.entries[id] = item
	q.bySeq[ticket.Seq] = item
	q.nextSeq = ticket.Seq
	return ticket, nil
}

func (q *Queue) Cancel(seq uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}
	item := q.bySeq[seq]
	if item == nil {
		return ErrNotFound
	}
	switch item.status {
	case statusCanceled:
		return ErrCanceled
	case statusReleased:
		return ErrReleased
	}

	if err := q.append(record{Type: recordCancel, Seq: seq}); err != nil {
		return err
	}
	item.status = statusCanceled
	return nil
}

func (q *Queue) ReleaseOne() (Ticket, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return Ticket{}, false, ErrQueueClosed
	}

	now := q.clock.Now()
	var next *entry
	for _, item := range q.bySeq {
		if item.status != statusActive || item.ticket.ReadyAt > now {
			continue
		}
		if next == nil || item.ticket.ReadyAt < next.ticket.ReadyAt ||
			(item.ticket.ReadyAt == next.ticket.ReadyAt && item.ticket.Seq < next.ticket.Seq) {
			next = item
		}
	}
	if next == nil {
		return Ticket{}, false, nil
	}

	if err := q.append(record{Type: recordRelease, Seq: next.ticket.Seq}); err != nil {
		return Ticket{}, false, err
	}
	next.status = statusReleased
	return next.ticket, true, nil
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}
	q.closed = true
	if q.file == nil {
		return nil
	}
	return q.file.Close()
}

func (q *Queue) recover() error {
	info, err := q.file.Stat()
	if err != nil {
		return err
	}

	validOffset := int64(0)
	buf := make([]byte, maxRecordPayload+recordCheckSize)
	for validOffset < info.Size() {
		if _, err := q.file.Seek(validOffset, io.SeekStart); err != nil {
			return err
		}

		if _, err := io.ReadFull(q.file, buf[:recordHeaderSize]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}
		payloadLen := int(binary.BigEndian.Uint32(buf[:recordHeaderSize]))
		if payloadLen == 0 || payloadLen > maxRecordPayload {
			break
		}

		framePayloadCheck := payloadLen + recordCheckSize
		if _, err := io.ReadFull(q.file, buf[:framePayloadCheck]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return err
		}

		payload := buf[:payloadLen]
		gotChecksum := binary.BigEndian.Uint64(buf[payloadLen : payloadLen+recordCheckSize])
		if gotChecksum != crc64.Checksum(payload, crc64Table) {
			break
		}

		var rec record
		if err := json.Unmarshal(payload, &rec); err != nil {
			break
		}
		if err := q.apply(rec); err != nil {
			return err
		}
		validOffset += int64(recordHeaderSize + framePayloadCheck)
	}

	if _, err := q.file.Seek(validOffset, io.SeekStart); err != nil {
		return err
	}
	if err := q.file.Truncate(validOffset); err != nil {
		return err
	}
	if validOffset != info.Size() {
		if err := q.file.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) apply(rec record) error {
	switch rec.Type {
	case recordRegister:
		if rec.Seq == 0 || rec.ID == "" || rec.Seq != q.nextSeq+1 {
			return ErrCorruptLog
		}
		if _, exists := q.entries[rec.ID]; exists {
			return ErrCorruptLog
		}
		item := &entry{
			ticket: Ticket{Seq: rec.Seq, ID: rec.ID, ReadyAt: rec.ReadyAt},
			status: statusActive,
		}
		q.entries[rec.ID] = item
		q.bySeq[rec.Seq] = item
		q.nextSeq = rec.Seq
	case recordCancel:
		item := q.bySeq[rec.Seq]
		if item == nil || item.status != statusActive {
			return ErrCorruptLog
		}
		item.status = statusCanceled
	case recordRelease:
		item := q.bySeq[rec.Seq]
		if item == nil || item.status != statusActive {
			return ErrCorruptLog
		}
		item.status = statusReleased
	default:
		return ErrCorruptLog
	}
	return nil
}

var crc64Table = crc64.MakeTable(crc64.ECMA)

func (q *Queue) append(rec record) error {
	if q.file == nil {
		return nil
	}

	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(payload) > maxRecordPayload {
		return errors.New("gatequeue: record is too large")
	}

	frame := make([]byte, 0, recordHeaderSize+len(payload)+recordCheckSize)
	lengthBytes := make([]byte, recordHeaderSize)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(payload)))
	frame = append(frame, lengthBytes...)
	frame = append(frame, payload...)
	checksumBytes := make([]byte, recordCheckSize)
	binary.BigEndian.PutUint64(checksumBytes, crc64.Checksum(payload, crc64Table))
	frame = append(frame, checksumBytes...)

	if _, err := q.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := io.Copy(q.file, bytes.NewReader(frame)); err != nil {
		q.closed = true
		return err
	}
	if err := q.file.Sync(); err != nil {
		q.closed = true
		return err
	}
	return nil
}
