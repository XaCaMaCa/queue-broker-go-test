package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{queues: make(map[string]*queue)}
}

func (b *broker) queue(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

type qSt struct {
	msgs    []string
	waiters []chan string
}

type queue struct {
	start sync.Once
	in    chan func(*qSt)
}

func (q *queue) init() {
	q.start.Do(func() {
		q.in = make(chan func(*qSt), 64)
		go func() {
			var st qSt
			for f := range q.in {
				f(&st)
			}
		}()
	})
}

func (q *queue) exec(f func(*qSt)) {
	q.init()
	q.in <- f
}

func (q *queue) execSync(f func(*qSt)) {
	done := make(chan struct{})
	q.exec(func(st *qSt) {
		f(st)
		close(done)
	})
	<-done
}

func (q *queue) enqueue(msg string) {
	q.execSync(func(st *qSt) {
		for len(st.waiters) > 0 {
			ch := st.waiters[0]
			st.waiters = st.waiters[1:]
			ch <- msg
			return
		}
		st.msgs = append(st.msgs, msg)
	})
}

func (q *queue) dequeue() (string, bool) {
	var msg string
	var ok bool
	synced := make(chan struct{})
	q.exec(func(st *qSt) {
		if len(st.msgs) > 0 {
			msg = st.msgs[0]
			st.msgs = st.msgs[1:]
			ok = true
		}
		close(synced)
	})
	<-synced
	return msg, ok
}

var errTimeout = errors.New("timeout")

func (q *queue) dequeueWait(d time.Duration) (string, error) {
	ch := make(chan string, 1)
	q.exec(func(st *qSt) {
		if len(st.msgs) > 0 {
			m := st.msgs[0]
			st.msgs = st.msgs[1:]
			ch <- m
			return
		}
		st.waiters = append(st.waiters, ch)
	})

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case msg := <-ch:
		return msg, nil
	case <-t.C:
		q.exec(func(st *qSt) {
			for i, w := range st.waiters {
				if w == ch {
					st.waiters = append(st.waiters[:i], st.waiters[i+1:]...)
					return
				}
			}
		})
		select {
		case msg := <-ch:
			return msg, nil
		default:
			return "", errTimeout
		}
	}
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: server <port>")
		os.Exit(1)
	}
	port := os.Args[1]
	if _, err := strconv.Atoi(port); err != nil {
		fmt.Fprintln(os.Stderr, "invalid port")
		os.Exit(1)
	}

	b := newBroker()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if len(path) < 2 || path[0] != '/' {
			http.NotFound(w, r)
			return
		}
		name := path[1:]
		if name == "" {
			http.NotFound(w, r)
			return
		}
		q := b.queue(name)

		switch r.Method {
		case http.MethodPut:
			v := r.URL.Query().Get("v")
			if v == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			q.enqueue(v)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			if ts := r.URL.Query().Get("timeout"); ts != "" {
				sec, err := strconv.Atoi(ts)
				if err != nil || sec < 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				msg, err := q.dequeueWait(time.Duration(sec) * time.Second)
				if err != nil {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(msg))
				return
			}
			msg, ok := q.dequeue()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(msg))

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
