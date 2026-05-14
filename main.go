package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	errTimeout  = errors.New("timeout")
	errShutdown = errors.New("broker is shutting down")
)

type qSt struct {
	msgs    []string
	waiters []chan string
}

type queue struct {
	start sync.Once
	in    chan func(*qSt)
	done  <-chan struct{}
}

func newQueue(done <-chan struct{}) *queue {
	return &queue{done: done}
}

func (q *queue) init() {
	q.start.Do(func() {
		q.in = make(chan func(*qSt), 64)
		go q.loop()
	})
}

func (q *queue) loop() {
	var st qSt
mainLoop:
	for {
		select {
		case f := <-q.in:
			f(&st)
		case <-q.done:
			for {
				select {
				case f := <-q.in:
					f(&st)
				default:
					break mainLoop
				}
			}
		}
	}
	for _, w := range st.waiters {
		close(w)
	}
}

func (q *queue) exec(f func(*qSt)) error {
	q.init()
	select {
	case <-q.done:
		return errShutdown
	default:
	}
	select {
	case q.in <- f:
		return nil
	case <-q.done:
		return errShutdown
	}
}

func (q *queue) execSync(f func(*qSt)) error {
	done := make(chan struct{})
	if err := q.exec(func(st *qSt) {
		f(st)
		close(done)
	}); err != nil {
		return err
	}
	select {
	case <-done:
		return nil
	case <-q.done:
		return errShutdown
	}
}

func (q *queue) enqueue(msg string) error {
	return q.execSync(func(st *qSt) {
		if len(st.waiters) > 0 {
			ch := st.waiters[0]
			st.waiters = st.waiters[1:]
			ch <- msg
			return
		}
		st.msgs = append(st.msgs, msg)
	})
}

func (q *queue) dequeue() (string, bool, error) {
	var msg string
	var ok bool
	synced := make(chan struct{})
	if err := q.exec(func(st *qSt) {
		if len(st.msgs) > 0 {
			msg = st.msgs[0]
			st.msgs = st.msgs[1:]
			ok = true
		}
		close(synced)
	}); err != nil {
		return "", false, err
	}
	select {
	case <-synced:
		return msg, ok, nil
	case <-q.done:
		return "", false, errShutdown
	}
}

func (q *queue) dequeueWait(d time.Duration) (string, error) {
	ch := make(chan string, 1)
	if err := q.exec(func(st *qSt) {
		if len(st.msgs) > 0 {
			m := st.msgs[0]
			st.msgs = st.msgs[1:]
			ch <- m
			return
		}
		st.waiters = append(st.waiters, ch)
	}); err != nil {
		return "", err
	}

	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case msg, ok := <-ch:
		if !ok {
			return "", errShutdown
		}
		return msg, nil
	case <-q.done:
		select {
		case msg, ok := <-ch:
			if ok {
				return msg, nil
			}
		default:
		}
		return "", errShutdown
	case <-t.C:
		removed := make(chan bool, 1)
		if err := q.exec(func(st *qSt) {
			for i, w := range st.waiters {
				if w == ch {
					st.waiters = append(st.waiters[:i], st.waiters[i+1:]...)
					removed <- true
					return
				}
			}
			removed <- false
		}); err != nil {
			select {
			case msg, ok := <-ch:
				if ok {
					return msg, nil
				}
			default:
			}
			return "", errShutdown
		}
		select {
		case <-removed:
		case <-q.done:
			return "", errShutdown
		}
		select {
		case msg, ok := <-ch:
			if !ok {
				return "", errShutdown
			}
			return msg, nil
		default:
			return "", errTimeout
		}
	}
}

type broker struct {
	mu        sync.Mutex
	queues    map[string]*queue
	done      chan struct{}
	closeOnce sync.Once
}

func newBroker() *broker {
	return &broker{
		queues: make(map[string]*queue),
		done:   make(chan struct{}),
	}
}

func (b *broker) queue(name string) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		q = newQueue(b.done)
		b.queues[name] = q
	}
	return q
}

func (b *broker) close() {
	b.closeOnce.Do(func() {
		close(b.done)
	})
}

func makeQueueHandler(b *broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, okPath := parseQueueName(r.URL.Path)
		if !okPath {
			http.NotFound(w, r)
			return
		}
		q := b.queue(name)

		switch r.Method {
		case http.MethodPut:
			handlePut(w, r, q)
		case http.MethodGet:
			handleGet(w, r, q)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func parseQueueName(path string) (string, bool) {
	if len(path) < 2 || path[0] != '/' {
		return "", false
	}
	name := path[1:]
	if name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}

func handlePut(w http.ResponseWriter, r *http.Request, q *queue) {
	values, ok := r.URL.Query()["v"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	v := ""
	if len(values) > 0 {
		v = values[0]
	}
	if err := q.enqueue(v); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	messagesEnqueuedTotal.Inc()
	w.WriteHeader(http.StatusOK)
}

func handleGet(w http.ResponseWriter, r *http.Request, q *queue) {
	if ts := r.URL.Query().Get("timeout"); ts != "" {
		sec, err := strconv.Atoi(ts)
		if err != nil || sec < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		waitStart := time.Now()
		msg, err := q.dequeueWait(time.Duration(sec) * time.Second)
		longPollWaitSeconds.Observe(time.Since(waitStart).Seconds())
		if err != nil {
			if errors.Is(err, errShutdown) {
				w.WriteHeader(http.StatusServiceUnavailable)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		writeBody(w, msg)
		return
	}
	msg, ok, err := q.dequeue()
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	writeBody(w, msg)
}

func writeBody(w http.ResponseWriter, msg string) {
	messagesDeliveredTotal.Inc()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(msg))
}

func newAppHandler(b *broker) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler())
	mux.Handle("/", makeQueueHandler(b))
	return metricsMiddleware(mux)
}

func newServer(addr string, b *broker) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           newAppHandler(b),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprintln(stderr, "usage: server <port>")
		return 1
	}
	port := args[1]
	if _, err := strconv.Atoi(port); err != nil {
		fmt.Fprintln(stderr, "invalid port")
		return 1
	}

	logger := slog.New(slog.NewJSONHandler(stdout, nil))
	slog.SetDefault(logger)

	b := newBroker()
	srv := newServer(":"+port, b)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			slog.Error("server failed", "err", err.Error())
			return 1
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err.Error())
	}

	b.close()
	slog.Info("server stopped")
	return 0
}

func main() {
	os.Exit(run(os.Args, os.Stdout, os.Stderr))
}
