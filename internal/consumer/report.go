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

type reportEvent struct {
	ProcessID string `json:"process_id"`
	ReportID  string `json:"report_id"`
	Event     string `json:"event"` // "report_created" | "report_failed"
	ErrorMsg  string `json:"error"`
}

type ReportConsumer struct {
	uc    *usecase.UpdateStatusUseCase
	nrApp *newrelic.Application
	log   *zap.Logger
}

func NewReportConsumer(uc *usecase.UpdateStatusUseCase, nrApp *newrelic.Application, log *zap.Logger) *ReportConsumer {
	return &ReportConsumer{uc: uc, nrApp: nrApp, log: log}
}

func (c *ReportConsumer) Run(ctx context.Context, deliveries <-chan amqp.Delivery) {
	c.log.Info("report consumer started")
	for {
		select {
		case <-ctx.Done():
			c.log.Info("report consumer stopped")
			return
		case d, ok := <-deliveries:
			if !ok {
				c.log.Warn("report consumer channel closed")
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
		c.log.Error("invalid report event payload", zap.Error(err))
		d.Nack(false, false)
		return
	}

	status := domain.StatusAnalyzed
	if evt.Event == "report_failed" {
		status = domain.StatusError
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	if err := c.uc.Execute(ctx, evt.ProcessID, status, evt.ReportID, evt.ErrorMsg); err != nil {
		c.log.Error("failed to update status from report event",
			zap.String("processId", evt.ProcessID),
			zap.Error(err),
		)
		txn.NoticeError(err)
		d.Nack(false, true)
		return
	}

	d.Ack(false)
}
