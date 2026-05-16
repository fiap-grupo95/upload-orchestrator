package consumer

import (
	"context"
	"encoding/json"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/newrelic/go-agent/v3/newrelic"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type processingEvent struct {
	ProcessID string `json:"process_id"`
	Event     string `json:"event"` // "processing_started" | "processing_error"
	ErrorMsg  string `json:"error"`
}

type ProcessingConsumer struct {
	uc    *usecase.UpdateStatusUseCase
	nrApp *newrelic.Application
	log   *zap.Logger
}

func NewProcessingConsumer(uc *usecase.UpdateStatusUseCase, nrApp *newrelic.Application, log *zap.Logger) *ProcessingConsumer {
	return &ProcessingConsumer{uc: uc, nrApp: nrApp, log: log}
}

func (c *ProcessingConsumer) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	c.log.Info("processing consumer started")
	for {
		select {
		case <-ctx.Done():
			c.log.Info("processing consumer stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				c.log.Warn("processing consumer channel closed")
				return
			}
			c.handle(d)
		}
	}
}

func (c *ProcessingConsumer) handle(d amqp.Delivery) {
	txn := c.nrApp.StartTransaction("consumer/processing-topic")
	defer txn.End()

	var evt processingEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		c.log.Error("invalid processing event payload", zap.Error(err))
		d.Nack(false, false)
		return
	}

	status := domain.StatusProcessing
	if evt.Event == "processing_error" {
		status = domain.StatusError
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	if err := c.uc.Execute(ctx, evt.ProcessID, status, "", evt.ErrorMsg); err != nil {
		c.log.Error("failed to update status from processing event",
			zap.String("processId", evt.ProcessID),
			zap.Error(err),
		)
		txn.NoticeError(err)
		d.Nack(false, true)
		return
	}

	d.Ack(false)
}
