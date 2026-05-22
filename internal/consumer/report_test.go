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
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

func runReportConsumerWithDeliveries(c *consumer.ReportConsumer, deliveries ...amqp.Delivery) {
	ch := make(chan amqp.Delivery, len(deliveries))
	for _, d := range deliveries {
		ch <- d
	}
	close(ch)
	c.Run(context.Background(), ch)
}

func TestReportConsumer_Run_InvalidJSON_Nacks(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewReportConsumer(uc, nrApp, log)

	ack := &mockAcknowledger{}
	runReportConsumerWithDeliveries(c, newDelivery(ack, []byte("{bad-json")))

	if ack.acked {
		t.Error("should NOT ack on invalid JSON")
	}
	if !ack.nacked {
		t.Error("should Nack on invalid JSON")
	}
	if ack.requeue {
		t.Error("should NOT requeue on invalid JSON")
	}
}

func TestReportConsumer_Run_ReportCreated_UpdatesStatusAnalyzedAndAcks(t *testing.T) {
	processID := uuid.New().String()
	var capturedStatus domain.ProcessStatus
	var capturedReportID string

	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			capturedStatus = status
			capturedReportID = reportID
			return nil
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewReportConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"report_id":  "report-xyz",
		"event":      "report_created",
	})
	ack := &mockAcknowledger{}
	runReportConsumerWithDeliveries(c, newDelivery(ack, body))

	if capturedStatus != domain.StatusAnalyzed {
		t.Errorf("expected StatusAnalyzed, got %q", capturedStatus)
	}
	if capturedReportID != "report-xyz" {
		t.Errorf("expected reportID %q, got %q", "report-xyz", capturedReportID)
	}
	if !ack.acked {
		t.Error("expected Ack on report_created")
	}
}

func TestReportConsumer_Run_ReportFailed_UpdatesStatusErrorAndAcks(t *testing.T) {
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
	c := consumer.NewReportConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"event":      "report_failed",
		"error":      "analysis timeout",
	})
	ack := &mockAcknowledger{}
	runReportConsumerWithDeliveries(c, newDelivery(ack, body))

	if capturedStatus != domain.StatusError {
		t.Errorf("expected StatusError, got %q", capturedStatus)
	}
	if capturedErrMsg != "analysis timeout" {
		t.Errorf("expected errMsg %q, got %q", "analysis timeout", capturedErrMsg)
	}
	if !ack.acked {
		t.Error("expected Ack on report_failed event")
	}
}

func TestReportConsumer_Run_UseCaseError_NacksWithRequeue(t *testing.T) {
	processID := uuid.New().String()
	repo := &consumerMockRepo{
		updateStatusFn: func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
			return fmt.Errorf("db timeout")
		},
	}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewReportConsumer(uc, nrApp, log)

	body, _ := json.Marshal(map[string]string{
		"process_id": processID,
		"report_id":  "report-abc",
		"event":      "report_created",
	})
	ack := &mockAcknowledger{}
	runReportConsumerWithDeliveries(c, newDelivery(ack, body))

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

func TestReportConsumer_Run_ContextCancelled_Stops(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewReportConsumer(uc, nrApp, log)

	ch := make(chan amqp.Delivery)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

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

func TestReportConsumer_Run_ChannelClosed_Stops(t *testing.T) {
	repo := &consumerMockRepo{}
	log := zap.NewNop()
	uc := usecase.NewUpdateStatusUseCase(repo, log)
	nrApp := newDisabledNRApp(t)
	c := consumer.NewReportConsumer(uc, nrApp, log)

	ch := make(chan amqp.Delivery)
	close(ch)

	c.Run(context.Background(), ch)
}

func TestReportConsumer_Run_MultipleMessages(t *testing.T) {
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
	c := consumer.NewReportConsumer(uc, nrApp, log)

	body1, _ := json.Marshal(map[string]string{"process_id": processID1, "report_id": "r1", "event": "report_created"})
	body2, _ := json.Marshal(map[string]string{"process_id": processID2, "report_id": "r2", "event": "report_created"})

	ack1, ack2 := &mockAcknowledger{}, &mockAcknowledger{}
	runReportConsumerWithDeliveries(c,
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
