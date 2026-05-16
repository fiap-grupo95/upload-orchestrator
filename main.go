package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/fiap/secure-systems/upload-orchestrator/internal/config"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/consumer"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/handler"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/queue"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/repository"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/storage"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/newrelic"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

	cfg, err := config.Load()
	if err != nil {
		log.Fatal("config load failed", zap.Error(err))
	}

	// ─── New Relic ────────────────────────────────────────────────────────────
	nrApp, err := newrelic.NewApplication(newrelic.ConfigFromEnvironment())
	if err != nil {
		log.Warn("new relic not configured", zap.Error(err))
		nrApp, _ = newrelic.NewApplication(newrelic.ConfigEnabled(false))
	}

	// ─── PostgreSQL (GORM) ────────────────────────────────────────────────────
	db, err := gorm.Open(postgres.Open(cfg.PostgresDSN), &gorm.Config{})
	if err != nil {
		log.Fatal("postgres connect failed", zap.Error(err))
	}
	processRepo := repository.NewProcessRepository(db)
	if err := processRepo.Migrate(); err != nil {
		log.Fatal("db migration failed", zap.Error(err))
	}

	// ─── MinIO ────────────────────────────────────────────────────────────────
	minioStore, err := storage.NewMinIOStorage(
		cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatal("minio init failed", zap.Error(err))
	}
	if err := minioStore.EnsureBucket(context.Background()); err != nil {
		log.Fatal("minio ensure bucket failed", zap.Error(err))
	}

	// ─── RabbitMQ ─────────────────────────────────────────────────────────────
	rmq, err := queue.NewRabbitMQ(cfg.RabbitMQURL)
	if err != nil {
		log.Fatal("rabbitmq connect failed", zap.Error(err))
	}
	defer rmq.Close()

	if err := rmq.DeclareQueue(cfg.ProcessQueue); err != nil {
		log.Fatal("declare process queue failed", zap.Error(err))
	}
	if err := rmq.DeclareExchange(cfg.ProcessingTopic); err != nil {
		log.Fatal("declare processing exchange failed", zap.Error(err))
	}
	if err := rmq.DeclareExchange(cfg.ReportTopic); err != nil {
		log.Fatal("declare report exchange failed", zap.Error(err))
	}

	processingQueue, err := rmq.BindQueue(cfg.ProcessingTopic)
	if err != nil {
		log.Fatal("bind processing queue failed", zap.Error(err))
	}
	reportQueue, err := rmq.BindQueue(cfg.ReportTopic)
	if err != nil {
		log.Fatal("bind report queue failed", zap.Error(err))
	}

	// ─── Casos de Uso ─────────────────────────────────────────────────────────
	uploadUC := usecase.NewUploadDiagramUseCase(processRepo, minioStore, rmq, cfg.ProcessQueue, log)
	getStatusUC := usecase.NewGetStatusUseCase(processRepo)
	updateStatusUC := usecase.NewUpdateStatusUseCase(processRepo, log)

	// ─── Consumers ────────────────────────────────────────────────────────────
	processingDeliveries, err := rmq.Consume(processingQueue)
	if err != nil {
		log.Fatal("consume processing queue failed", zap.Error(err))
	}
	reportDeliveries, err := rmq.Consume(reportQueue)
	if err != nil {
		log.Fatal("consume report queue failed", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go consumer.NewProcessingConsumer(updateStatusUC, nrApp, log).Run(ctx, processingDeliveries)
	go consumer.NewReportConsumer(updateStatusUC, nrApp, log).Run(ctx, reportDeliveries)

	// ─── HTTP (Gin) ───────────────────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(nrgin.Middleware(nrApp))

	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	internal := r.Group("/internal")
	{
		diagramH := handler.NewDiagramHandler(uploadUC, log)
		statusH := handler.NewStatusHandler(getStatusUC, log)

		internal.POST("/diagrams", diagramH.Upload)
		internal.GET("/process/:processId/status", statusH.GetStatus)
	}

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("upload-orchestrator started", zap.String("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
	log.Info("upload-orchestrator stopped")
}
