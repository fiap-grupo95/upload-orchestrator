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
	"github.com/fiap/secure-systems/upload-orchestrator/internal/logging"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/queue"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/repository"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/storage"
	"github.com/fiap/secure-systems/upload-orchestrator/internal/usecase"
	"github.com/gin-gonic/gin"
	"github.com/newrelic/go-agent/v3/integrations/nrgin"
	"github.com/newrelic/go-agent/v3/newrelic"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic("config load failed: " + err.Error())
	}

	// ─── New Relic ────────────────────────────────────────────────────────────
	nrApp, err := newrelic.NewApplication(
		newrelic.ConfigFromEnvironment(),
		newrelic.ConfigDistributedTracerEnabled(true),
		newrelic.ConfigAppLogForwardingEnabled(true),
	)
	if err != nil {
		nrApp, _ = newrelic.NewApplication(newrelic.ConfigEnabled(false))
	}

	// ─── Logging (deve ser inicializado após o New Relic) ─────────────────────
	logging.Init(nrApp)
	log := logging.Logger()

	// ─── PostgreSQL (GORM) ────────────────────────────────────────────────────
	db, err := gorm.Open(postgres.Open(cfg.PostgresDSN), &gorm.Config{})
	if err != nil {
		log.Fatal().Err(err).Msg("postgres connect failed")
	}
	processRepo := repository.NewProcessRepository(db)
	if err := processRepo.Migrate(); err != nil {
		log.Fatal().Err(err).Msg("db migration failed")
	}

	// ─── MinIO ────────────────────────────────────────────────────────────────
	minioStore, err := storage.NewMinIOStorage(
		cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket, cfg.MinioUseSSL,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("minio init failed")
	}
	if err := minioStore.EnsureBucket(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("minio ensure bucket failed")
	}

	// ─── RabbitMQ ─────────────────────────────────────────────────────────────
	rmq, err := queue.NewRabbitMQ(cfg.RabbitMQURL)
	if err != nil {
		log.Fatal().Err(err).Msg("rabbitmq connect failed")
	}
	defer rmq.Close()

	if err := rmq.DeclareQueue(cfg.ProcessQueue); err != nil {
		log.Fatal().Err(err).Msg("declare process queue failed")
	}
	if err := rmq.DeclareExchange(cfg.ProcessingTopic); err != nil {
		log.Fatal().Err(err).Msg("declare processing exchange failed")
	}
	if err := rmq.DeclareExchange(cfg.ReportTopic); err != nil {
		log.Fatal().Err(err).Msg("declare report exchange failed")
	}

	processingQueue, err := rmq.BindQueue(cfg.ProcessingTopic)
	if err != nil {
		log.Fatal().Err(err).Msg("bind processing queue failed")
	}
	reportQueue, err := rmq.BindQueue(cfg.ReportTopic)
	if err != nil {
		log.Fatal().Err(err).Msg("bind report queue failed")
	}

	// ─── Casos de Uso ─────────────────────────────────────────────────────────
	uploadUC := usecase.NewUploadDiagramUseCase(processRepo, minioStore, rmq, cfg.ProcessQueue)
	getStatusUC := usecase.NewGetStatusUseCase(processRepo)
	updateStatusUC := usecase.NewUpdateStatusUseCase(processRepo)

	// ─── Consumers ────────────────────────────────────────────────────────────
	processingDeliveries, err := rmq.Consume(processingQueue)
	if err != nil {
		log.Fatal().Err(err).Msg("consume processing queue failed")
	}
	reportDeliveries, err := rmq.Consume(reportQueue)
	if err != nil {
		log.Fatal().Err(err).Msg("consume report queue failed")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go consumer.NewProcessingConsumer(updateStatusUC, nrApp).Run(ctx, processingDeliveries)
	go consumer.NewReportConsumer(updateStatusUC, nrApp).Run(ctx, reportDeliveries)

	// ─── HTTP (Gin) ───────────────────────────────────────────────────────────
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(nrgin.Middleware(nrApp))

	r.GET("/ping", func(c *gin.Context) { c.String(http.StatusOK, "pong") })

	internal := r.Group("/internal")
	{
		diagramH := handler.NewDiagramHandler(uploadUC)
		statusH := handler.NewStatusHandler(getStatusUC)

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
		log.Info().Str("port", cfg.Port).Msg("upload-orchestrator started")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal().Err(err).Msg("server error")
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("shutdown error")
	}
	log.Info().Msg("upload-orchestrator stopped")
}
