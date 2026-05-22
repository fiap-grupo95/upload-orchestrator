package consumer

import (
	"context"
	"encoding/json"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/newrelic/go-agent/v3/newrelic"
	amqp "github.com/rabbitmq/amqp091-go"
)

type processingEvent struct {
	ProcessID string `json:"process_id"`
	Event     string `json:"event"` // "processing_started" | "processing_error"
	ErrorMsg  string `json:"error"`
}

type ProcessingConsumer struct {
	uc    *usecase.UpdateStatusUseCase
	nrApp *newrelic.Application
}

func NewProcessingConsumer(uc *usecase.UpdateStatusUseCase, nrApp *newrelic.Application) *ProcessingConsumer {
	return &ProcessingConsumer{uc: uc, nrApp: nrApp}
}

func (c *ProcessingConsumer) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	logging.Logger().Info().Msg("processing consumer started")
	for {
		select {
		case <-ctx.Done():
			logging.Logger().Info().Msg("processing consumer stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				logging.Logger().Warn().Msg("processing consumer channel closed")
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
		logging.Logger().Error().
			Err(err).
			Int("body_size", len(d.Body)).
			Msg("invalid processing event payload")
		txn.NoticeError(err)
		d.Nack(false, false)
		return
	}

	status := domain.StatusProcessing
	if evt.Event == "processing_error" {
		status = domain.StatusError
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	txn.AddAttribute("process_id", evt.ProcessID)
	txn.AddAttribute("event", evt.Event)

	log := logging.LoggerWithContext(ctx).With().
		Str("process_id", evt.ProcessID).
		Str("event", evt.Event).
		Logger()

	if err := c.uc.Execute(ctx, evt.ProcessID, status, "", evt.ErrorMsg); err != nil {
		log.Error().Err(err).Msg("failed to update status from processing event")
		txn.NoticeError(err)
		d.Nack(false, true)
		return
	}

	log.Info().Str("new_status", string(status)).Msg("process status updated")
	d.Ack(false)
}
