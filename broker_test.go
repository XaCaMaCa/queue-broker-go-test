package main

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBrokerSameQueueInstance(t *testing.T) {
	b := newBroker()
	defer b.close()

	q1 := b.queue("foo")
	q2 := b.queue("foo")
	if q1 != q2 {
		t.Fatal("expected same queue instance for same name")
	}
}

func TestBrokerDifferentQueues(t *testing.T) {
	b := newBroker()
	defer b.close()

	q1 := b.queue("foo")
	q2 := b.queue("bar")
	if q1 == q2 {
		t.Fatal("expected different queue instances")
	}
}

func TestQueueEnqueueDequeueBasic(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	if err := q.enqueue("hello"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	msg, ok, err := q.dequeue()
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if msg != "hello" {
		t.Fatalf("got %q, want %q", msg, "hello")
	}
}

func TestQueueDequeueEmpty(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	_, ok, err := q.dequeue()
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false on empty queue")
	}
}

func TestQueueFIFOOrder(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	msgs := []string{"a", "b", "c", "d", "e"}
	for _, m := range msgs {
		if err := q.enqueue(m); err != nil {
			t.Fatalf("enqueue %s: %v", m, err)
		}
	}

	for _, expected := range msgs {
		msg, ok, err := q.dequeue()
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if !ok {
			t.Fatal("queue exhausted unexpectedly")
		}
		if msg != expected {
			t.Fatalf("FIFO violation: got %q, want %q", msg, expected)
		}
	}
}

func TestQueueDequeueWaitImmediate(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	if err := q.enqueue("hello"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	start := time.Now()
	msg, err := q.dequeueWait(5 * time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("dequeueWait: %v", err)
	}
	if msg != "hello" {
		t.Fatalf("got %q, want %q", msg, "hello")
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("dequeueWait took too long: %v", elapsed)
	}
}

func TestQueueDequeueWaitTimeout(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	start := time.Now()
	_, err := q.dequeueWait(200 * time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, errTimeout) {
		t.Fatalf("expected errTimeout, got %v", err)
	}
	if elapsed < 200*time.Millisecond {
		t.Fatalf("returned before timeout: %v", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("returned much after timeout: %v", elapsed)
	}
}

func TestQueueDequeueWaitReceiveBeforeTimeout(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = q.enqueue("delayed")
	}()

	start := time.Now()
	msg, err := q.dequeueWait(2 * time.Second)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("dequeueWait: %v", err)
	}
	if msg != "delayed" {
		t.Fatalf("got %q, want %q", msg, "delayed")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("took too long: %v", elapsed)
	}
}

func TestQueueMultipleWaitersFIFO(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	const n = 3
	results := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msg, err := q.dequeueWait(3 * time.Second)
			if err != nil {
				t.Errorf("waiter %d: %v", idx, err)
				return
			}
			results[idx] = msg
		}(i)
		time.Sleep(30 * time.Millisecond)
	}

	expected := []string{"first", "second", "third"}
	for _, m := range expected {
		if err := q.enqueue(m); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	wg.Wait()

	for i, exp := range expected {
		if results[i] != exp {
			t.Fatalf("waiter %d got %q, want %q", i, results[i], exp)
		}
	}
}

func TestBrokerCloseUnblocksWaiters(t *testing.T) {
	b := newBroker()
	q := b.queue("test")

	errCh := make(chan error, 1)
	go func() {
		_, err := q.dequeueWait(10 * time.Second)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	b.close()

	select {
	case err := <-errCh:
		if !errors.Is(err, errShutdown) {
			t.Fatalf("expected errShutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not unblocked after close")
	}
}

func TestBrokerCloseIdempotent(t *testing.T) {
	b := newBroker()
	b.close()
	b.close()
}

func TestEnqueueAfterCloseReturnsError(t *testing.T) {
	b := newBroker()
	q := b.queue("test")
	b.close()

	time.Sleep(20 * time.Millisecond)

	if err := q.enqueue("x"); !errors.Is(err, errShutdown) {
		t.Fatalf("expected errShutdown, got %v", err)
	}
}
