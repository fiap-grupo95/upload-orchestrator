package consumer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/consumer"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// mockAcknowledger implementa amqp.Acknowledger para testes.
type mockAcknowledger struct {
	acked   bool
	nacked  bool
	requeue bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.acked = true
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nacked = true
	m.requeue = requeue
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

func newDisabledNRApp(t *testing.T) *newrelic.Application {
	t.Helper()
	app, err := newrelic.NewApplication(newrelic.ConfigEnabled(false))
	if err != nil {
		t.Fatalf("failed to create disabled New Relic app: %v", err)
	}
	return app
}

func newDelivery(ack *mockAcknowledger, body []byte) amqp.Delivery {
	return amqp.Delivery{
		Acknowledger: ack,
		Body:         body,
	}
}

func runWithDeliveries(c *consumer.ProcessingConsumer, deliveries ...amqp.Delivery) {
	ch := make(chan amqp.Delivery, len(deliveries))
	for _, d := range deliveries {
		ch <- d
	}
	close(ch) // fecha o canal para que Run retorne após processar todas as mensagens
	c.Run(context.Background(), ch)
}

func TestProcessingConsumer_Run_InvalidJSON_Nacks(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	ack := &mockAcknowledger{}
	runWithDeliveries(c, newDelivery(ack, []byte("invalid-json{")))

	if ack.acked {
		t.Error("should NOT ack on invalid JSON")
	}
	if !ack.nacked {
		t.Error("should Nack on invalid JSON")
	}
	if ack.requeue {
		t.Error("should NOT requeue on invalid JSON (dead-letter)")
	}
}

func TestProcessingConsumer_Run_ProcessingStarted_UpdatesStatusAndAcks(t *testing.T) {
	processID := uuid.New().String()
	var capturedStatus domain.ProcessStatus

	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			capturedStatus = status
			return nil
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"event":      "processing_started",
	})
	ack := &mockAcknowledger{}
	runWithDeliveries(c, newDelivery(ack, body))

	if capturedStatus != domain.StatusProcessing {
		t.Errorf("expected StatusProcessing, got %q", capturedStatus)
	}
	if !ack.acked {
		t.Error("expected Ack on successful processing")
	}
}

func TestProcessingConsumer_Run_ProcessingError_UpdatesStatusErrorAndAcks(t *testing.T) {
	processID := uuid.New().String()
	var capturedStatus domain.ProcessStatus
	var capturedErrMsg string

	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			capturedStatus = status
			capturedErrMsg = errMsg
			return nil
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"event":      "processing_error",
		"error":      "out of memory",
	})
	ack := &mockAcknowledger{}
	runWithDeliveries(c, newDelivery(ack, body))

	if capturedStatus != domain.StatusError {
		t.Errorf("expected StatusError, got %q", capturedStatus)
	}
	if capturedErrMsg != "out of memory" {
		t.Errorf("expected errMsg %q, got %q", "out of memory", capturedErrMsg)
	}
	if !ack.acked {
		t.Error("expected Ack on processing_error event")
	}
}

func TestProcessingConsumer_Run_UseCaseError_NacksWithRequeue(t *testing.T) {
	processID := uuid.New().String()
	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			return fmt.Errorf("db unavailable")
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"event":      "processing_started",
	})
	ack := &mockAcknowledger{}
	runWithDeliveries(c, newDelivery(ack, body))

	if ack.acked {
		t.Error("should NOT ack when usecase fails")
	}
	if !ack.nacked {
		t.Error("should Nack when usecase fails")
	}
	if !ack.requeue {
		t.Error("should requeue=true when usecase fails (retry)")
	}
}

func TestProcessingConsumer_Run_ContextCancelled_Stops(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	ch := make(chan amqp.Delivery) // canal vazio, não fechado

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancela imediatamente

	done := make(chan struct{})
	go func() {
		c.Run(ctx, ch)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestProcessingConsumer_Run_ChannelClosed_Stops(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	ch := make(chan amqp.Delivery)
	close(ch) // fecha imediatamente

	// Run deve retornar quando o canal é fechado
	c.Run(context.Background(), ch)
}

func TestProcessingConsumer_Run_MultipleMessages(t *testing.T) {
	processID1 := uuid.New().String()
	processID2 := uuid.New().String()
	var processedIDs []string

	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			processedIDs = append(processedIDs, id)
			return nil
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewProcessingConsumer(uc, nrApp, log)

	body1, _ := json.Marshal(map[string]string{"process_id": processID1, "event": "processing_started"})
	body2, _ := json.Marshal(map[string]string{"process_id": processID2, "event": "processing_started"})

	ack1, ack2 := &mockAcknowledger{}, &mockAcknowledger{}
	runWithDeliveries(c,
		newDelivery(ack1, body1),
		newDelivery(ack2, body2),
	)

	if len(processedIDs) != 2 {
		t.Errorf("expected 2 messages processed, got %d", len(processedIDs))
	}
	if !ack1.acked || !ack2.acked {
		t.Error("both messages should be acked")
	}
}
