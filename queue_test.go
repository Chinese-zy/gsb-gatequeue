package gatequeue

import (
	"encoding/binary"
	"hash/crc64"
	"os"
	"path/filepath"
	"testing"
)

type fixedClock struct{ now int64 }

func (c fixedClock) Now() int64 { return c.now }

func TestNotReadyCannotRelease(t *testing.T) {
	q, err := New("", fixedClock{now: 9})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.Register("a", 10); err != nil {
		t.Fatal(err)
	}
	if ticket, ok, err := q.ReleaseOne(); err != nil || ok || ticket != (Ticket{}) {
		t.Fatalf("ReleaseOne before ready = %+v, %v, %v", ticket, ok, err)
	}
}

func TestSameTimeReleasesByRegistrationOrder(t *testing.T) {
	q, err := New("", fixedClock{now: 5})
	if err != nil {
		t.Fatal(err)
	}

	first, err := q.Register("first", 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Register("second", 10)
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("sequences = %d, %d, want 1, 2", first.Seq, second.Seq)
	}

	released := releaseAll(t, q, 10)
	if got := []string{released[0].ID, released[1].ID}; got[0] != "first" || got[1] != "second" {
		t.Fatalf("release order = %v, want [first second]", got)
	}
}

func TestEarlierReadyCanReleaseAndSameTimeKeepsOrder(t *testing.T) {
	q, err := New("", fixedClock{now: 5})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := q.Register("later-registered-early", 8); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Register("same-time-a", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Register("same-time-b", 10); err != nil {
		t.Fatal(err)
	}

	q.clock = fixedClock{now: 8}
	ticket, ok, err := q.ReleaseOne()
	if err != nil || !ok || ticket.ID != "later-registered-early" {
		t.Fatalf("first release = %+v, %v, %v", ticket, ok, err)
	}
	if ticket, ok, err := q.ReleaseOne(); err != nil || ok || ticket != (Ticket{}) {
		t.Fatalf("future release = %+v, %v, %v", ticket, ok, err)
	}

	released := releaseAll(t, q, 10)
	if got := []string{released[0].ID, released[1].ID}; got[0] != "same-time-a" || got[1] != "same-time-b" {
		t.Fatalf("release order = %v", got)
	}
}

func TestCancelKeepsLaterSequence(t *testing.T) {
	q, err := New("", fixedClock{now: 10})
	if err != nil {
		t.Fatal(err)
	}

	first, err := q.Register("first", 10)
	if err != nil {
		t.Fatal(err)
	}
	canceled, err := q.Register("canceled", 10)
	if err != nil {
		t.Fatal(err)
	}
	third, err := q.Register("third", 10)
	if err != nil {
		t.Fatal(err)
	}

	if err := q.Cancel(canceled.Seq); err != nil {
		t.Fatal(err)
	}
	released := releaseAll(t, q, 10)
	if len(released) != 2 {
		t.Fatalf("released %d tickets, want 2", len(released))
	}
	if released[0] != first || released[1] != third {
		t.Fatalf("released = %+v, want %+v then %+v", released, first, third)
	}
	if third.Seq != 3 {
		t.Fatalf("later sequence = %d, want 3", third.Seq)
	}
}

func TestRecoveryKeepsCompleteRecordsAndDropsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	clock := fixedClock{now: 10}

	q, err := New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	first, err := q.Register("first", 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Register("second", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	torn := makeTornRegisterFrame(t, 3, "torn", 10)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(torn[:len(torn)/2]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	q, err = New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	tornAgain, err := q.Register("torn", 10)
	if err != nil {
		t.Fatalf("torn record was not discarded: %v", err)
	}
	released := releaseAll(t, q, 10)
	if len(released) != 3 {
		t.Fatalf("released %d tickets, want 3", len(released))
	}
	if released[0] != first || released[1] != second || released[2] != tornAgain {
		t.Fatalf("release order = %+v", released)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q, err = New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	if ticket, ok, err := q.ReleaseOne(); err != nil || ok || ticket != (Ticket{}) {
		t.Fatalf("recovered queue released a ticket again: %+v, %v, %v", ticket, ok, err)
	}
	if tornAgain, ok := q.bySeq[3]; !ok || tornAgain.ticket.ID != "torn" || tornAgain.status != statusReleased {
		t.Fatalf("sequence 3 after recovery = %+v, %v", tornAgain, ok)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseIsDurableAndDuplicateIDRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	clock := fixedClock{now: 10}

	q, err := New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Register("same", 10); err != nil {
		t.Fatal(err)
	}
	ticket, ok, err := q.ReleaseOne()
	if err != nil || !ok || ticket.ID != "same" {
		t.Fatalf("ReleaseOne = %+v, %v, %v", ticket, ok, err)
	}
	if _, err := q.Register("same", 10); err != ErrDuplicate {
		t.Fatalf("duplicate after release error = %v, want %v", err, ErrDuplicate)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q, err = New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	if ticket, ok, err := q.ReleaseOne(); err != nil || ok || ticket != (Ticket{}) {
		t.Fatalf("released again = %+v, %v, %v", ticket, ok, err)
	}
	if _, err := q.Register("same", 10); err != ErrDuplicate {
		t.Fatalf("duplicate after recovery error = %v, want %v", err, ErrDuplicate)
	}
}

func TestCancelIsDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.log")
	clock := fixedClock{now: 10}

	q, err := New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Register("a", 10); err != nil {
		t.Fatal(err)
	}
	canceled, err := q.Register("b", 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Register("c", 10); err != nil {
		t.Fatal(err)
	}
	if err := q.Cancel(canceled.Seq); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	q, err = New(path, clock)
	if err != nil {
		t.Fatal(err)
	}
	released := releaseAll(t, q, 10)
	if got := []string{released[0].ID, released[1].ID}; got[0] != "a" || got[1] != "c" {
		t.Fatalf("released = %v, want [a c]", got)
	}
}

func releaseAll(t *testing.T, q *Queue, now int64) []Ticket {
	t.Helper()
	q.clock = fixedClock{now: now}
	var released []Ticket
	for {
		ticket, ok, err := q.ReleaseOne()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return released
		}
		released = append(released, ticket)
	}
}

func makeTornRegisterFrame(t *testing.T, seq uint64, id string, readyAt int64) []byte {
	t.Helper()
	payload := []byte(`{"type":"register","seq":` + uintToString(seq) + `,"id":"` + id + `","ready_at":` + uintToString(uint64(readyAt)) + `}`)
	frame := make([]byte, 0, 4+len(payload)+8)
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(payload)))
	frame = append(frame, length...)
	frame = append(frame, payload...)
	checksum := make([]byte, 8)
	binary.BigEndian.PutUint64(checksum, crc64.Checksum(payload, crc64Table))
	return append(frame, checksum...)
}

func uintToString(v uint64) string {
	if v == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	return string(digits[i:])
}
