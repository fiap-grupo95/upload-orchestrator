package usecase

import (
	"context"
	"io"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
)

// mockProcessRepo é uma implementação mock de ProcessRepository para uso nos testes.
type mockProcessRepo struct {
	createFn       func(ctx context.Context, p *domain.Process) error
	findByIDFn     func(ctx context.Context, id string) (*domain.Process, error)
	updateStatusFn func(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error
}

func (m *mockProcessRepo) Create(ctx context.Context, p *domain.Process) error {
	if m.createFn != nil {
		return m.createFn(ctx, p)
	}
	return nil
}

func (m *mockProcessRepo) FindByID(ctx context.Context, id string) (*domain.Process, error) {
	if m.findByIDFn != nil {
		return m.findByIDFn(ctx, id)
	}
	return nil, nil
}

func (m *mockProcessRepo) UpdateStatus(ctx context.Context, id string, status domain.ProcessStatus, reportID, errMsg string) error {
	if m.updateStatusFn != nil {
		return m.updateStatusFn(ctx, id, status, reportID, errMsg)
	}
	return nil
}

// mockDiagramStorage é uma implementação mock de DiagramStorage.
type mockDiagramStorage struct {
	uploadFn func(ctx context.Context, key string, r io.Reader, size int64, contentType string) error
}

func (m *mockDiagramStorage) Upload(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
	if m.uploadFn != nil {
		return m.uploadFn(ctx, key, r, size, contentType)
	}
	return nil
}

// mockEventPublisher é uma implementação mock de EventPublisher.
type mockEventPublisher struct {
	publishFn func(ctx context.Context, queue string, payload []byte) error
}

func (m *mockEventPublisher) Publish(ctx context.Context, queue string, payload []byte) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, queue, payload)
	}
	return nil
}
