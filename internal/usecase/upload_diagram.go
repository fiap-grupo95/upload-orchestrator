package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/google/uuid"
)

type UploadDiagramInput struct {
	Content     io.Reader
	Size        int64
	ContentType string
	Filename    string
}

type UploadDiagramOutput struct {
	ProcessID string
	Status    domain.ProcessStatus
	CreatedAt time.Time
}

type ProcessQueueMessage struct {
	ProcessID   string `json:"process_id"`
	S3Key       string `json:"s3_key"`
	ContentType string `json:"content_type"`
}

type UploadDiagramUseCase struct {
	repo      ProcessRepository
	storage   DiagramStorage
	publisher EventPublisher
	queue     string
}

func NewUploadDiagramUseCase(
	repo ProcessRepository,
	storage DiagramStorage,
	publisher EventPublisher,
	queue string,
) *UploadDiagramUseCase {
	return &UploadDiagramUseCase{repo: repo, storage: storage, publisher: publisher, queue: queue}
}

func (uc *UploadDiagramUseCase) Execute(ctx context.Context, in UploadDiagramInput) (*UploadDiagramOutput, error) {
	log := logging.LoggerWithContext(ctx)
	processID := uuid.New().String()
	s3Key := fmt.Sprintf("diagrams/%s", processID)

	defer logging.StartSegment(ctx, "UploadDiagram.StorageUpload")()
	if err := uc.storage.Upload(ctx, s3Key, in.Content, in.Size, in.ContentType); err != nil {
		log.Error().Str("process_id", processID).Err(err).Msg("failed to upload to storage")
		return nil, fmt.Errorf("storage upload: %w", err)
	}

	defer logging.StartSegment(ctx, "UploadDiagram.RepositoryCreate")()
	p := &domain.Process{
		ID:          processID,
		S3Key:       s3Key,
		ContentType: in.ContentType,
		Status:      domain.StatusReceived,
	}
	if err := uc.repo.Create(ctx, p); err != nil {
		log.Error().Str("process_id", processID).Err(err).Msg("failed to persist process")
		return nil, fmt.Errorf("repository create: %w", err)
	}

	defer logging.StartSegment(ctx, "UploadDiagram.QueuePublish")()
	msg := ProcessQueueMessage{ProcessID: processID, S3Key: s3Key, ContentType: in.ContentType}
	payload, _ := json.Marshal(msg)
	if err := uc.publisher.Publish(ctx, uc.queue, payload); err != nil {
		log.Error().Str("process_id", processID).Err(err).Msg("failed to publish to queue")
		return nil, fmt.Errorf("queue publish: %w", err)
	}

	log.Info().Str("process_id", processID).Msg("diagram uploaded and queued")
	return &UploadDiagramOutput{ProcessID: processID, Status: domain.StatusReceived, CreatedAt: p.CreatedAt}, nil
}
