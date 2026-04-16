package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

type service struct {
	brokers []string
	writers map[string]*kafka.Writer
}

type event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type movieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int      `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

type userEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username,omitempty"`
	Email     string    `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type paymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType string    `json:"method_type,omitempty"`
}

type eventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     event  `json:"event"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	svc := newService(getEnv("KAFKA_BROKERS", "localhost:9092"))
	defer svc.close()

	svc.startConsumers()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", healthHandler)
	mux.HandleFunc("/api/events/movie", svc.handleMovieEvent)
	mux.HandleFunc("/api/events/user", svc.handleUserEvent)
	mux.HandleFunc("/api/events/payment", svc.handlePaymentEvent)

	port := getEnv("PORT", "8082")
	addr := ":" + port
	log.Printf("starting events service on %s with brokers=%s", addr, strings.Join(svc.brokers, ","))
	log.Fatal(http.ListenAndServe(addr, mux))
}

func newService(brokers string) *service {
	brokerList := splitAndTrim(brokers)
	return &service{
		brokers: brokerList,
		writers: map[string]*kafka.Writer{
			movieTopic:   newWriter(brokerList, movieTopic),
			userTopic:    newWriter(brokerList, userTopic),
			paymentTopic: newWriter(brokerList, paymentTopic),
		},
	}
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
	}
}

func (s *service) close() {
	for topic, writer := range s.writers {
		if err := writer.Close(); err != nil {
			log.Printf("close writer for %s: %v", topic, err)
		}
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (s *service) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload movieEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if payload.MovieID == 0 || strings.TrimSpace(payload.Title) == "" || strings.TrimSpace(payload.Action) == "" {
		writeJSONError(w, http.StatusBadRequest, "movie_id, title and action are required")
		return
	}

	evt := event{
		ID:        fmt.Sprintf("movie-%d-%d", payload.MovieID, time.Now().UnixNano()),
		Type:      "movie",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}

	s.publishAndRespond(w, evt, movieTopic)
}

func (s *service) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload userEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if payload.UserID == 0 || strings.TrimSpace(payload.Action) == "" {
		writeJSONError(w, http.StatusBadRequest, "user_id and action are required")
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	}

	evt := event{
		ID:        fmt.Sprintf("user-%d-%d", payload.UserID, time.Now().UnixNano()),
		Type:      "user",
		Timestamp: payload.Timestamp.UTC(),
		Payload:   payload,
	}

	s.publishAndRespond(w, evt, userTopic)
}

func (s *service) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var payload paymentEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if payload.PaymentID == 0 || payload.UserID == 0 || payload.Amount == 0 || strings.TrimSpace(payload.Status) == "" {
		writeJSONError(w, http.StatusBadRequest, "payment_id, user_id, amount and status are required")
		return
	}
	if payload.Timestamp.IsZero() {
		payload.Timestamp = time.Now().UTC()
	}

	evt := event{
		ID:        fmt.Sprintf("payment-%d-%d", payload.PaymentID, time.Now().UnixNano()),
		Type:      "payment",
		Timestamp: payload.Timestamp.UTC(),
		Payload:   payload,
	}

	s.publishAndRespond(w, evt, paymentTopic)
}

func (s *service) publishAndRespond(w http.ResponseWriter, evt event, topic string) {
	response, err := s.publishEvent(topic, evt)
	if err != nil {
		log.Printf("publish %s event failed: %v", topic, err)
		writeJSONError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *service) publishEvent(topic string, evt event) (eventResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	value, err := json.Marshal(evt)
	if err != nil {
		return eventResponse{}, err
	}

	message := kafka.Message{
		Key:   []byte(evt.ID),
		Value: value,
		Time:  evt.Timestamp,
	}

	if err := s.writers[topic].WriteMessages(ctx, message); err != nil {
		return eventResponse{}, err
	}

	offset, partition, err := fetchTopicPosition(ctx, s.brokers, topic)
	if err != nil {
		return eventResponse{}, err
	}

	log.Printf("published %s event id=%s topic=%s partition=%d offset=%d", evt.Type, evt.ID, topic, partition, offset)

	return eventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     evt,
	}, nil
}

func fetchTopicPosition(ctx context.Context, brokers []string, topic string) (int64, int, error) {
	conn, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, 0)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	lastOffset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, 0, err
	}

	offset := lastOffset - 1
	if offset < 0 {
		offset = 0
	}

	return offset, 0, nil
}

func (s *service) startConsumers() {
	for topic := range s.writers {
		go s.consumeTopic(topic)
	}
}

func (s *service) consumeTopic(topic string) {
	groupID := "events-service-" + strings.ReplaceAll(topic, "_", "-")

	for {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     s.brokers,
			GroupID:     groupID,
			Topic:       topic,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
			Dialer: &kafka.Dialer{
				Timeout:   10 * time.Second,
				DualStack: true,
			},
		})

		err := s.readLoop(reader, topic)
		if err != nil {
			log.Printf("consumer for %s stopped: %v", topic, err)
		}

		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("close reader for %s: %v", topic, closeErr)
		}

		time.Sleep(3 * time.Second)
	}
}

func (s *service) readLoop(reader *kafka.Reader, topic string) error {
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}

		var evt event
		if err := json.Unmarshal(message.Value, &evt); err != nil {
			log.Printf("invalid event in topic=%s partition=%d offset=%d: %v", topic, message.Partition, message.Offset, err)
			continue
		}

		log.Printf("consumed event type=%s id=%s topic=%s partition=%d offset=%d payload=%s",
			evt.Type, evt.ID, topic, message.Partition, message.Offset, string(message.Value))
	}
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	if len(result) == 0 {
		result = append(result, "localhost:9092")
	}
	return result
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
