FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /upload-orchestrator ./main.go

FROM alpine:3.21
RUN adduser -D -u 1000 appuser
COPY --from=builder /upload-orchestrator /upload-orchestrator
USER appuser
EXPOSE 8081
ENTRYPOINT ["/upload-orchestrator"]
