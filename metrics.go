package main

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Метрики PayPath / queuebroker: без имён очередей в лейблах (высокая кардинальность).

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "queuebroker_http_requests_total",
			Help: "HTTP-запросы по методу и коду ответа.",
		},
		[]string{"method", "code"},
	)

	longPollWaitSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "queuebroker_longpoll_wait_seconds",
			Help: "Длительность long-poll GET с валидным timeout (от старта ожидания до ответа).",
			Buckets: []float64{
				0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120,
			},
		},
	)

	messagesEnqueuedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "queuebroker_messages_enqueued_total",
			Help: "Сообщения успешно приняты методом PUT.",
		},
	)

	messagesDeliveredTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "queuebroker_messages_delivered_total",
			Help: "Сообщения отданы клиенту ответом 200 с телом.",
		},
	)
)

// statusRecorder фиксирует код ответа для Prometheus.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func newStatusRecorder(w http.ResponseWriter) *statusRecorder {
	return &statusRecorder{ResponseWriter: w, code: http.StatusOK}
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// metricsMiddleware считает каждый HTTP-запрос после выполнения внутреннего handler.
func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := newStatusRecorder(w)
		next.ServeHTTP(rec, r)
		httpRequestsTotal.WithLabelValues(r.Method, strconv.Itoa(rec.code)).Inc()
	})
}

func metricsHandler() http.Handler {
	return promhttp.Handler()
}
