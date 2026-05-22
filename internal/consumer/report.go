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

type reportEvent struct {
	ProcessID string `json:"process_id"`
	ReportID  string `json:"report_id"`
	Event     string `json:"event"` // "report_created" | "report_failed"
	ErrorMsg  string `json:"error"`
}

type ReportConsumer struct {
	uc    *usecase.UpdateStatusUseCase
	nrApp *newrelic.Application
}

func NewReportConsumer(uc *usecase.UpdateStatusUseCase, nrApp *newrelic.Application) *ReportConsumer {
	return &ReportConsumer{uc: uc, nrApp: nrApp}
}

func (c *ReportConsumer) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	logging.Logger().Info().Msg("report consumer started")
	for {
		select {
		case <-ctx.Done():
			logging.Logger().Info().Msg("report consumer stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				logging.Logger().Warn().Msg("report consumer channel closed")
				return
			}
			c.handle(d)
		}
	}
}

func (c *ReportConsumer) handle(d amqp.Delivery) {
	txn := c.nrApp.StartTransaction("consumer/report-topic")
	defer txn.End()

	var evt reportEvent
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		logging.Logger().Error().Err(err).Msg("invalid report event payload")
		d.Nack(false, false)
		return
	}

	status := domain.StatusAnalyzed
	if evt.Event == "report_failed" {
		status = domain.StatusError
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	txn.AddAttribute("process_id", evt.ProcessID)
	if err := c.uc.Execute(ctx, evt.ProcessID, status, evt.ReportID, evt.ErrorMsg); err != nil {
		logging.LoggerWithContext(ctx).Error().
			Str("process_id", evt.ProcessID).Err(err).
			Msg("failed to update status from report event")
		txn.NoticeError(err)
		d.Nack(false, true)
		return
	}

	d.Ack(false)
}
