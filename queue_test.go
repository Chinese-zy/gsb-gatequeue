package gatequeue

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// fakeClock is an injectable clock: tests drive time explicitly, no sleep.
type fakeClock struct {
	mu  sync.Mutex
	now int64
}

func (c *fakeClock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// Fixed timeline for all tests (Unix nanos).
const (
	t0 = int64(1_700_000_000_000_000_000)
	t1 = t0 + 10
	t2 = t0 + 20
	t3 = t0 + 30
)

func openAt(t *testing.T, clock Clock) (*Queue, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.wal")
	q, err := Open(path, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return q, path
}

func mustEnqueue(t *testing.T, q *Queue, id string, at int64) Entry {
	t.Helper()
	e, dup, err := q.Enqueue(id, at, []byte("payload-"+id))
	if err != nil {
		t.Fatalf("Enqueue(%s): %v", id, err)
	}
	if dup {
		t.Fatalf("Enqueue(%s): unexpected dup", id)
	}
	return e
}

func mustPop(t *testing.T, q *Queue) []Entry {
	t.Helper()
	out, err := q.PopReady()
	if err != nil {
		t.Fatalf("PopReady: %v", err)
	}
	return out
}

func ids(entries []Entry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.ID
	}
	return out
}

func TestNotReadyCannotRelease(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	mustEnqueue(t, q, "a", t2)
	if got := mustPop(t, q); len(got) != 0 {
		t.Fatalf("before due time, popped %v", ids(got))
	}
	clk.Set(t2 - 1)
	if got := mustPop(t, q); len(got) != 0 {
		t.Fatalf("one nanos early, popped %v", ids(got))
	}
	clk.Set(t2)
	if got := mustPop(t, q); !reflect.DeepEqual(ids(got), []string{"a"}) {
		t.Fatalf("at due time, popped %v", ids(got))
	}
}

func TestSameTimeReleasedInRegistrationOrder(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	for _, id := range []string{"first", "second", "third"} {
		mustEnqueue(t, q, id, t1)
	}
	clk.Set(t1)
	if got := mustPop(t, q); !reflect.DeepEqual(ids(got), []string{"first", "second", "third"}) {
		t.Fatalf("same-time order = %v", ids(got))
	}
}

func TestEarlierTimeWinsRegardlessOfRegistrationOrder(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	mustEnqueue(t, q, "late-registered-early-due", t1)
	mustEnqueue(t, q, "early-registered-late-due", t3)
	mustEnqueue(t, q, "mid", t2)
	clk.Set(t3)
	if got := mustPop(t, q); !reflect.DeepEqual(ids(got),
		[]string{"late-registered-early-due", "mid", "early-registered-late-due"}) {
		t.Fatalf("order = %v", ids(got))
	}
}

func TestCancelDoesNotShiftSequences(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	a := mustEnqueue(t, q, "a", t1)
	mustEnqueue(t, q, "b", t1)
	c := mustEnqueue(t, q, "c", t1)
	if a.Seq != 1 || c.Seq != 3 {
		t.Fatalf("initial seqs: a=%d c=%d", a.Seq, c.Seq)
	}

	ok, err := q.Cancel("b")
	if err != nil || !ok {
		t.Fatalf("Cancel(b) = %v, %v", ok, err)
	}
	seq, ok := q.SeqOf("c")
	if !ok || seq != 3 {
		t.Fatalf("after cancel, c seq = %d, %v; want 3, true", seq, ok)
	}

	// New registrations keep counting up; cancelled seq 2 is never reused.
	d := mustEnqueue(t, q, "d", t1)
	if d.Seq != 4 {
		t.Fatalf("d seq = %d, want 4", d.Seq)
	}

	clk.Set(t1)
	if got := mustPop(t, q); !reflect.DeepEqual(ids(got), []string{"a", "c", "d"}) {
		t.Fatalf("after cancel, popped %v", ids(got))
	}
}

func TestCancelUnknownID(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	ok, err := q.Cancel("nope")
	if err != nil || ok {
		t.Fatalf("Cancel(nope) = %v, %v", ok, err)
	}
}

func TestDuplicateEnqueueNeverReleasedTwice(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, _ := openAt(t, clk)
	defer q.Close()

	first := mustEnqueue(t, q, "x", t1)
	again, dup, err := q.Enqueue("x", t2, []byte("other"))
	if err != nil || !dup {
		t.Fatalf("re-Enqueue = dup %v, err %v", dup, err)
	}
	if again.Seq != first.Seq || again.ReleaseAt != first.ReleaseAt {
		t.Fatalf("re-Enqueue mutated entry: %+v vs %+v", again, first)
	}
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1", q.Len())
	}

	clk.Set(t2)
	got := mustPop(t, q)
	if !reflect.DeepEqual(ids(got), []string{"x"}) {
		t.Fatalf("popped %v", ids(got))
	}
	if got := mustPop(t, q); len(got) != 0 {
		t.Fatalf("released twice: %v", ids(got))
	}
}

func TestCrashRecoveryKeepsCompleteDropsTorn(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, path := openAt(t, clk)

	mustEnqueue(t, q, "one", t1)
	mustEnqueue(t, q, "two", t2)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a process killed mid-write: half a third record at the tail.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	full := encodeAdd(Entry{ID: "three", Seq: 3, ReleaseAt: t3})
	if _, err := f.Write(full[:len(full)/2]); err != nil {
		t.Fatalf("write torn record: %v", err)
	}
	f.Close()

	clk.Set(t3)
	q2, err := Open(path, clk)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()

	got := mustPop(t, q2)
	if !reflect.DeepEqual(ids(got), []string{"one", "two"}) {
		t.Fatalf("recovered pops = %v, want [one two]", ids(got))
	}
	if q2.Len() != 0 {
		t.Fatalf("torn entry survived recovery, Len = %d", q2.Len())
	}

	// The torn tail must be truncated so the next seq (3) can be reused
	// cleanly by a fresh registration without log corruption.
	e := mustEnqueue(t, q2, "three", t3)
	if e.Seq != 3 {
		t.Fatalf("re-registered seq = %d, want 3", e.Seq)
	}
	if got := mustPop(t, q2); !reflect.DeepEqual(ids(got), []string{"three"}) {
		t.Fatalf("after re-register, popped %v", ids(got))
	}
}

func TestReopenReplaysInOriginalOrder(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, path := openAt(t, clk)
	mustEnqueue(t, q, "a", t1)
	mustEnqueue(t, q, "b", t1)
	mustEnqueue(t, q, "c", t1)
	q.Close()

	clk.Set(t1)
	q2, err := Open(path, clk)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if got := mustPop(t, q2); !reflect.DeepEqual(ids(got), []string{"a", "b", "c"}) {
		t.Fatalf("replayed order = %v", ids(got))
	}
}

func TestReleasedAndCancelledStayGoneAfterReopen(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, path := openAt(t, clk)
	mustEnqueue(t, q, "released", t1)
	mustEnqueue(t, q, "cancelled", t2)
	mustEnqueue(t, q, "survivor", t3)

	clk.Set(t1)
	if got := mustPop(t, q); !reflect.DeepEqual(ids(got), []string{"released"}) {
		t.Fatalf("first pop = %v", ids(got))
	}
	if ok, _ := q.Cancel("cancelled"); !ok {
		t.Fatal("Cancel(cancelled) failed")
	}
	q.Close()

	clk.Set(t3)
	q2, err := Open(path, clk)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	got := mustPop(t, q2)
	if !reflect.DeepEqual(ids(got), []string{"survivor"}) {
		t.Fatalf("after reopen, popped %v, want [survivor]", ids(got))
	}
	if got := mustPop(t, q2); len(got) != 0 {
		t.Fatalf("entry released twice across reopen: %v", ids(got))
	}
}

func TestCorruptChecksumDropsTail(t *testing.T) {
	clk := &fakeClock{now: t0}
	q, path := openAt(t, clk)
	mustEnqueue(t, q, "good", t1)
	q.Close()

	// Append a record with a bad checksum (bit-flipped body).
	bad := encodeAdd(Entry{ID: "bad", Seq: 2, ReleaseAt: t1})
	bad[6] ^= 0xFF
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write(bad)
	f.Close()

	clk.Set(t1)
	q2, err := Open(path, clk)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close()
	if got := mustPop(t, q2); !reflect.DeepEqual(ids(got), []string{"good"}) {
		t.Fatalf("popped %v, want [good]", ids(got))
	}
}
