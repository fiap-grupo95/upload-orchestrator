package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/domain"
)

func TestUploadDiagramUseCase_Execute_Success(t *testing.T) {
	var uploadedKey, uploadedContentType string
	var createdProcess *domain.Process
	var publishedQueue string
	var publishedPayload []byte

	storage := &mockDiagramStorage{
		uploadFn: func(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
			uploadedKey = key
			uploadedContentType = contentType
			return nil
		},
	}
	repo := &mockProcessRepo{
		createFn: func(ctx context.Context, p *domain.Process) error {
			createdProcess = p
			return nil
		},
	}
	publisher := &mockEventPublisher{
		publishFn: func(ctx context.Context, queue string, payload []byte) error {
			publishedQueue = queue
			publishedPayload = payload
			return nil
		},
	}

	uc := NewUploadDiagramUseCase(repo, storage, publisher, "process.queue")

	in := UploadDiagramInput{
		Content:     strings.NewReader("diagram-content"),
		Size:        15,
		ContentType: "image/png",
		Filename:    "arch.png",
	}

	out, err := uc.Execute(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.ProcessID == "" {
		t.Error("ProcessID should not be empty")
	}
	if out.Status != domain.StatusReceived {
		t.Errorf("expected status %q, got %q", domain.StatusReceived, out.Status)
	}

	// Verifica que a key do storage começa com "diagrams/"
	if !strings.HasPrefix(uploadedKey, "diagrams/") {
		t.Errorf("expected s3Key to start with 'diagrams/', got %q", uploadedKey)
	}
	if uploadedContentType != "image/png" {
		t.Errorf("expected contentType %q, got %q", "image/png", uploadedContentType)
	}

	// Verifica que o processo foi criado com os campos corretos
	if createdProcess == nil {
		t.Fatal("expected repo.Create to be called")
	}
	if createdProcess.Status != domain.StatusReceived {
		t.Errorf("expected process status %q, got %q", domain.StatusReceived, createdProcess.Status)
	}
	if createdProcess.ContentType != "image/png" {
		t.Errorf("expected process contentType %q, got %q", "image/png", createdProcess.ContentType)
	}

	// Verifica que a mensagem foi publicada na fila correta
	if publishedQueue != "process.queue" {
		t.Errorf("expected queue %q, got %q", "process.queue", publishedQueue)
	}

	var msg ProcessQueueMessage
	if err := json.Unmarshal(publishedPayload, &msg); err != nil {
		t.Fatalf("failed to unmarshal published payload: %v", err)
	}
	if msg.ProcessID != out.ProcessID {
		t.Errorf("published ProcessID %q != output ProcessID %q", msg.ProcessID, out.ProcessID)
	}
	if msg.S3Key != uploadedKey {
		t.Errorf("published S3Key %q != uploaded key %q", msg.S3Key, uploadedKey)
	}
	if msg.ContentType != "image/png" {
		t.Errorf("published ContentType %q != expected %q", msg.ContentType, "image/png")
	}
}

func TestUploadDiagramUseCase_Execute_StorageError_ReturnsError(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	var repoCalled, publisherCalled bool

	storage := &mockDiagramStorage{
		uploadFn: func(ctx context.Context, key string, r io.Reader, size int64, contentType string) error {
			return storageErr
		},
	}
	repo := &mockProcessRepo{
		createFn: func(ctx context.Context, p *domain.Process) error {
			repoCalled = true
			return nil
		},
	}
	publisher := &mockEventPublisher{
		publishFn: func(ctx context.Context, queue string, payload []byte) error {
			publisherCalled = true
			return nil
		},
	}

	uc := NewUploadDiagramUseCase(repo, storage, publisher, "process.queue")

	_, err := uc.Execute(context.Background(), UploadDiagramInput{
		Content:     strings.NewReader("data"),
		ContentType: "image/png",
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, storageErr) {
		t.Errorf("expected error to wrap %v, got %v", storageErr, err)
	}
	if repoCalled {
		t.Error("repo.Create should NOT be called when storage fails")
	}
	if publisherCalled {
		t.Error("publisher.Publish should NOT be called when storage fails")
	}
}

func TestUploadDiagramUseCase_Execute_RepoError_ReturnsError(t *testing.T) {
	repoErr := errors.New("db error")
	var publisherCalled bool

	storage := &mockDiagramStorage{}
	repo := &mockProcessRepo{
		createFn: func(ctx context.Context, p *domain.Process) error {
			return repoErr
		},
	}
	publisher := &mockEventPublisher{
		publishFn: func(ctx context.Context, queue string, payload []byte) error {
			publisherCalled = true
			return nil
		},
	}

	uc := NewUploadDiagramUseCase(repo, storage, publisher, "process.queue")

	_, err := uc.Execute(context.Background(), UploadDiagramInput{
		Content:     strings.NewReader("data"),
		ContentType: "application/pdf",
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, repoErr) {
		t.Errorf("expected error to wrap %v, got %v", repoErr, err)
	}
	if publisherCalled {
		t.Error("publisher.Publish should NOT be called when repo fails")
	}
}

func TestUploadDiagramUseCase_Execute_PublisherError_ReturnsError(t *testing.T) {
	publishErr := errors.New("rabbitmq down")

	storage := &mockDiagramStorage{}
	repo := &mockProcessRepo{}
	publisher := &mockEventPublisher{
		publishFn: func(ctx context.Context, queue string, payload []byte) error {
			return publishErr
		},
	}

	uc := NewUploadDiagramUseCase(repo, storage, publisher, "process.queue")

	_, err := uc.Execute(context.Background(), UploadDiagramInput{
		Content:     strings.NewReader("data"),
		ContentType: "image/jpeg",
	})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, publishErr) {
		t.Errorf("expected error to wrap %v, got %v", publishErr, err)
	}
}

func TestUploadDiagramUseCase_Execute_GeneratesUniqueProcessIDs(t *testing.T) {
	storage := &mockDiagramStorage{}
	repo := &mockProcessRepo{}
	publisher := &mockEventPublisher{}

	uc := NewUploadDiagramUseCase(repo, storage, publisher, "queue")

	out1, err := uc.Execute(context.Background(), UploadDiagramInput{Content: strings.NewReader("a"), ContentType: "image/png"})
	if err != nil {
		t.Fatalf("first execute error: %v", err)
	}
	out2, err := uc.Execute(context.Background(), UploadDiagramInput{Content: strings.NewReader("b"), ContentType: "image/png"})
	if err != nil {
		t.Fatalf("second execute error: %v", err)
	}
	if out1.ProcessID == out2.ProcessID {
		t.Error("each Execute call should generate a unique ProcessID")
	}
}
