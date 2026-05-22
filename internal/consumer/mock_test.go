package consumer_test

import (
	"context"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
)

// consumerMockRepo implementa usecase.ProcessRepository para todos os testes do pacote consumer.
type consumerMockRepo struct {
	createFn       func(ctx context.Context, p *domain.Process) error
	findByIDFn     func(ctx context.Context, id string) (*domain.Process, error)
	updateStatusFn func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error
}

func (m *consumerMockRepo) Create(ctx context.Context, p *domain.Process) error {
	if m.createFn != nil {
		return m.createFn(ctx, p)
	}
	return nil
}

func (m *consumerMockRepo) FindByID(ctx context.Context, id string) (*domain.Process, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}
	return nil, nil
}

func (m *consumerMockRepo) UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status, reportID, errMsg)
	}
	return nil
}
