#!/usr/bin/env bash
# =============================================================================
# Upload Orchestrator — Documentação de Endpoints Internos
# Base URL: http://localhost:8081
# ATENÇÃO: estes endpoints são chamados pelo API Gateway, não pelo cliente final
# =============================================================================

BASE_URL="http://localhost:8081"
PROCESS_ID="${PROCESS_ID:-SEU_PROCESS_ID_AQUI}"

# ─── Health Check ─────────────────────────────────────────────────────────────
curl -i "${BASE_URL}/ping"

# Resposta: 200 pong

# ─── Upload de Diagrama (chamado pelo Gateway) ────────────────────────────────
# POST /internal/diagrams
# Body: conteúdo binário do arquivo
# Headers obrigatórios:
#   Content-Type: MIME type do arquivo
#   Content-Length: tamanho em bytes (importante para o MinIO)
#   X-Filename: nome sanitizado do arquivo
#   X-Request-ID: ID de rastreamento propagado pelo Gateway
#
FILE="/caminho/para/diagrama.png"
FILE_SIZE=$(wc -c < "${FILE}")

curl -i -X POST "${BASE_URL}/internal/diagrams" \
  -H "Content-Type: image/png" \
  -H "Content-Length: ${FILE_SIZE}" \
  -H "X-Filename: diagrama.png" \
  -H "X-Request-ID: test-orch-001" \
  --data-binary "@${FILE}"

# Resposta esperada (202 Accepted):
# {
#   "process_id": "550e8400-e29b-41d4-a716-446655440000",
#   "status": "RECEBIDO",
#   "created_at": "2026-05-16T22:00:00Z"
# }

# ─── Consulta de Status (chamado pelo Gateway) ────────────────────────────────
# GET /internal/process/:processId/status
# processId deve ser um UUID válido
#
curl -i "${BASE_URL}/internal/process/${PROCESS_ID}/status" \
  -H "X-Request-ID: test-orch-002"

# Resposta esperada (200 OK):
# {
#   "process_id": "550e8400-...",
#   "status": "EM_PROCESSAMENTO"
# }

# Resposta com ERRO:
# {
#   "process_id": "550e8400-...",
#   "status": "ERRO",
#   "error": "llm analysis: llm response failed guardrail validation: risks array is empty"
# }

# ─── Verificação no PostgreSQL ────────────────────────────────────────────────
# Conecte ao banco para inspecionar o estado diretamente:
#
#   docker exec -it hacka-postgres-1 psql -U orchestrator -d orchestrator -c \
#     "SELECT id, status, report_id, created_at, updated_at FROM processes ORDER BY created_at DESC LIMIT 10;"
#

# ─── Verificação no MinIO ─────────────────────────────────────────────────────
# Acesse o console: http://localhost:9001
# Usuário: minioadmin | Senha: dev_minio_pass (do .env)
# Bucket: diagrams
#
# Via CLI (requer mc instalado):
#   mc alias set local http://localhost:9000 minioadmin dev_minio_pass
#   mc ls local/diagrams/

# ─── Casos de Erro ────────────────────────────────────────────────────────────

# processId com formato inválido → 400
curl -i "${BASE_URL}/internal/process/nao-e-uuid/status" \
  -H "X-Request-ID: test-err-001"

# processId UUID inexistente → 404
curl -i "${BASE_URL}/internal/process/00000000-0000-0000-0000-000000000000/status" \
  -H "X-Request-ID: test-err-002"
