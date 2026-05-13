package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentEnqueueDequeueNoLoss(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	const producers = 10
	const messagesPerProducer = 100
	total := producers * messagesPerProducer

	var produceWG sync.WaitGroup
	for i := 0; i < producers; i++ {
		produceWG.Add(1)
		go func(id int) {
			defer produceWG.Done()
			for j := 0; j < messagesPerProducer; j++ {
				if err := q.enqueue(fmt.Sprintf("p%d-m%d", id, j)); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(i)
	}
	produceWG.Wait()

	seen := make(map[string]bool, total)
	for i := 0; i < total; i++ {
		msg, ok, err := q.dequeue()
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if !ok {
			t.Fatalf("expected message at i=%d", i)
		}
		if seen[msg] {
			t.Fatalf("duplicate message: %q", msg)
		}
		seen[msg] = true
	}

	if _, ok, _ := q.dequeue(); ok {
		t.Fatal("queue should be empty after all dequeues")
	}
}

func TestConcurrentMultipleConsumers(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	const messages = 500
	for i := 0; i < messages; i++ {
		if err := q.enqueue(fmt.Sprintf("m%d", i)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	const consumers = 20
	results := make(chan string, messages)
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, ok, err := q.dequeue()
				if err != nil || !ok {
					return
				}
				results <- msg
			}
		}()
	}
	wg.Wait()
	close(results)

	collected := make(map[string]bool)
	for m := range results {
		if collected[m] {
			t.Fatalf("duplicate: %q", m)
		}
		collected[m] = true
	}
	if len(collected) != messages {
		t.Fatalf("got %d unique, want %d", len(collected), messages)
	}
}

func TestConcurrentWaitersAndProducers(t *testing.T) {
	b := newBroker()
	defer b.close()
	q := b.queue("test")

	const n = 50
	got := make(chan string, n)

	var waitersWG sync.WaitGroup
	for i := 0; i < n; i++ {
		waitersWG.Add(1)
		go func() {
			defer waitersWG.Done()
			msg, err := q.dequeueWait(5 * 1000 * 1000 * 1000)
			if err != nil {
				t.Errorf("waiter: %v", err)
				return
			}
			got <- msg
		}()
	}

	var producersWG sync.WaitGroup
	for i := 0; i < n; i++ {
		producersWG.Add(1)
		go func(idx int) {
			defer producersWG.Done()
			if err := q.enqueue(fmt.Sprintf("m%d", idx)); err != nil {
				t.Errorf("enqueue: %v", err)
			}
		}(i)
	}

	producersWG.Wait()
	waitersWG.Wait()
	close(got)

	seen := make(map[string]bool, n)
	for m := range got {
		if seen[m] {
			t.Fatalf("duplicate: %q", m)
		}
		seen[m] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d unique messages, want %d", len(seen), n)
	}
}
