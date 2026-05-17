# Upload Orchestrator

Serviço responsável por receber diagramas, armazená-los com segurança e orquestrar o ciclo de vida do processamento assíncrono via máquina de estados.

---

## Descrição do Problema

Após a validação no gateway, o arquivo precisa ser persistido com durabilidade e o processo de análise precisa ser iniciado de forma confiável. Além disso, o cliente precisa consultar o progresso de uma operação assíncrona (análise por IA pode levar dezenas de segundos) sem fazer polling cego.

**Desafios específicos endereçados:**

- Garantir que o arquivo está seguro no object storage antes de iniciar o processamento
- Manter o estado do processo de forma consistente mesmo em caso de falha de serviços downstream
- Receber notificações de múltiplos serviços (processing e report) para atualizar o estado corretamente
- Expor consulta de status de forma eficiente sem acoplamento direto com os processadores

---

## Arquitetura Proposta

```mermaid
graph TD
    GW["API Gateway"]

    subgraph UO["Upload Orchestrator :8081"]
        HD["handler/diagram.go\n→ usecase/upload_diagram.go"]
        HS["handler/status.go\n→ usecase/get_status.go"]
        UC["usecase/update_status.go"]
        CP["consumer/processing.go"]
        CR["consumer/report.go"]
        CP --> UC
        CR --> UC
    end

    PG[("PostgreSQL (GORM)")]
    MN[("MinIO")]
    RMQ["RabbitMQ\npublish + consume"]

    GW -->|"POST /internal/diagrams"| HD
    GW -->|"GET /internal/process/:id/status"| HS
    HD --> PG
    HD --> MN
    HD --> RMQ
    HS --> PG
    UC --> PG
    RMQ --> CP
    RMQ --> CR
```

### Máquina de estados

```mermaid
stateDiagram-v2
    [*] --> RECEBIDO
    RECEBIDO --> EM_PROCESSAMENTO
    EM_PROCESSAMENTO --> ANALISADO
    RECEBIDO --> ERRO
    EM_PROCESSAMENTO --> ERRO
```

| Transição | Gatilho |
|---|---|
| `RECEBIDO` | Upload salvo no MinIO + registro criado no PostgreSQL |
| `EM_PROCESSAMENTO` | Evento `processing_started` no `processing.topic` |
| `ANALISADO` | Evento `report_created` no `report.topic` (inclui `report_id`) |
| `ERRO` | Evento `processing_error` ou `report_failed` em qualquer tópico |

### Camadas internas (Clean Architecture)

```
internal/
├── domain/
│   └── process.go       ← Process, ProcessStatus, ErrProcessNotFound, ErrInvalidID
├── usecase/
│   ├── ports.go         ← ProcessRepository, DiagramStorage, EventPublisher (interfaces)
│   ├── upload_diagram.go← gera UUID → MinIO → PostgreSQL → process.queue
│   ├── get_status.go    ← valida UUID → PostgreSQL
│   └── update_status.go ← valida UUID → UPDATE PostgreSQL
├── repository/
│   └── process_repo.go  ← GORM + AutoMigrate; Where("id = ?", id) — sem SQL raw
├── storage/
│   └── minio.go         ← EnsureBucket na startup, PutObject
├── queue/
│   └── rabbitmq.go      ← DeclareQueue, DeclareExchange (fanout), BindQueue, Consume
├── consumer/
│   ├── processing.go    ← processing.topic → EM_PROCESSAMENTO ou ERRO
│   └── report.go        ← report.topic → ANALISADO (+ report_id) ou ERRO
└── handler/
    ├── diagram.go       ← POST /internal/diagrams
    └── status.go        ← GET /internal/process/:processId/status
```

### Topologia RabbitMQ

```mermaid
graph LR
    UO["upload-orchestrator"]
    PQ["process.queue\n(direct)"]
    PT["processing.topic\n(fanout)"]
    RT["report.topic\n(fanout)"]

    UO -->|publica| PQ
    PT -->|"fila exclusiva"| UO
    RT -->|"fila exclusiva"| UO
```

---

## Fluxo da Solução

```mermaid
flowchart TD
    subgraph POST["POST /internal/diagrams"]
        P1["Lê Content-Type e X-Filename dos headers"]
        P2["Upload MinIO: diagrams/{uuid}"]
        P3["Cria registro PostgreSQL\nstatus = RECEBIDO"]
        P4["Publica process.queue\n{process_id, s3_key, content_type}"]
        P5["Retorna 202\n{process_id, status, created_at}"]
        P1 --> P2 --> P3 --> P4 --> P5
    end

    subgraph PT["processing.topic consumer"]
        PT1{event?}
        PT2["UPDATE status = EM_PROCESSAMENTO"]
        PT3["UPDATE status = ERRO, error_msg"]
        PT1 -->|processing_started| PT2
        PT1 -->|processing_error| PT3
    end

    subgraph RT["report.topic consumer"]
        RT1{event?}
        RT2["UPDATE status = ANALISADO\n+ report_id"]
        RT3["UPDATE status = ERRO, error_msg"]
        RT1 -->|report_created| RT2
        RT1 -->|report_failed| RT3
    end

    subgraph GET["GET /internal/process/:processId/status"]
        G1["uuid.Parse(processId)"]
        G2{válido?}
        G3["400 Bad Request"]
        G4["PostgreSQL FindByID"]
        G5{encontrado?}
        G6["404 Not Found"]
        G7["200 {process_id, status, report_id?, error?}"]
        G1 --> G2
        G2 -->|não| G3
        G2 -->|sim| G4
        G4 --> G5
        G5 -->|não| G6
        G5 -->|sim| G7
    end
```

---

## Instruções de Execução

### Variáveis de ambiente

| Variável | Obrigatório | Padrão | Descrição |
|---|---|---|---|
| `POSTGRES_DSN` | Sim | — | Ex: `postgres://user:pass@host:5432/db?sslmode=disable` |
| `MINIO_ENDPOINT` | Sim | — | Ex: `minio:9000` |
| `MINIO_ACCESS_KEY` | Sim | — | Chave de acesso MinIO |
| `MINIO_SECRET_KEY` | Sim | — | Chave secreta MinIO |
| `RABBITMQ_URL` | Sim | — | Ex: `amqp://guest:pass@rabbitmq:5672/` |
| `PORT` | Não | `8081` | Porta HTTP |
| `MINIO_BUCKET` | Não | `diagrams` | Nome do bucket |
| `MINIO_USE_SSL` | Não | `false` | Habilitar TLS no MinIO |
| `PROCESS_QUEUE` | Não | `process.queue` | Fila de trabalho |
| `PROCESSING_TOPIC` | Não | `processing.topic` | Exchange de feedback do processing-service |
| `REPORT_TOPIC` | Não | `report.topic` | Exchange de feedback do report-service |
| `NEW_RELIC_LICENSE_KEY` | Não | — | Chave New Relic |
| `NEW_RELIC_APP_NAME` | Não | — | Nome da app no New Relic |

### Executar com Docker Compose (recomendado)

```bash
# A partir da raiz do projeto
docker compose up --build -d upload-orchestrator

# Verificar saúde
curl http://localhost:8081/ping

# Acompanhar logs
docker logs -f hacka-upload-orchestrator-1
```

### Executar localmente (desenvolvimento)

```bash
cd upload-orchestrator
go mod download

export POSTGRES_DSN="postgres://orchestrator:dev_postgres_pass@localhost:5432/orchestrator?sslmode=disable"
export MINIO_ENDPOINT="localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="dev_minio_pass"
export RABBITMQ_URL="amqp://guest:dev_rabbitmq_pass@localhost:5672/"

go run main.go
```

### Inspecionar o PostgreSQL

```bash
docker exec -it hacka-postgres-1 psql -U orchestrator -d orchestrator \
  -c "SELECT id, status, report_id, created_at FROM processes ORDER BY created_at DESC LIMIT 10;"
```

### Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/ping` | Healthcheck |
| `POST` | `/internal/diagrams` | Recebe diagrama do gateway |
| `GET` | `/internal/process/:processId/status` | Consulta status |

---

## Segurança

### Requisitos básicos adotados

| Controle | Implementação |
|---|---|
| Validação de UUID | `uuid.Parse` antes de qualquer consulta ao PostgreSQL — previne IDOR e queries mal-formadas |
| Queries parametrizadas | GORM com `Where("id = ?", id)` e `Updates(map[string]any{...})` — sem SQL raw (CWE-89) |
| Chave S3 gerada internamente | `diagrams/{uuid_gerado_pelo_servidor}` — sem componentes de caminho controlados pelo usuário |
| Credenciais via env | DSN do PostgreSQL, endpoint e credenciais do MinIO, URL do RabbitMQ — sem hardcode |
| Nack sem requeue | Mensagens com payload inválido ou campo ausente são descartadas definitivamente — sem loop infinito |
| AutoMigrate controlado | `db.AutoMigrate` apenas na startup; sem DDL durante operação normal |

### Validação de entradas não confiáveis

- **`processId` (URL):** `uuid.Parse(processId)` retorna `ErrInvalidID` antes de qualquer acesso ao banco — 400 ao cliente.
- **Mensagens RabbitMQ (`processing.topic`, `report.topic`):** `json.Unmarshal` seguido de validação de `ProcessID` não vazio; `Nack(false, false)` em payload inválido.
- **Conteúdo do arquivo:** recebido do gateway já validado (MIME, tamanho); a chave S3 é gerada pelo servidor (UUID) — sem entrada do usuário na path do objeto.
- **Atualização de status:** `UpdateStatus` aceita apenas os campos `status`, `report_id` e `error_msg` via map tipado — sem injeção de campos arbitrários.

### Comunicação entre serviços

- Publicação no `process.queue` com `delivery_mode: Persistent` — mensagens sobrevivem a restart do RabbitMQ.
- Consumo de `processing.topic` e `report.topic` via filas exclusivas efêmeras (`exclusive: true`) — sem vazamento de mensagens entre instâncias.
- `Content-Length` explícito no upload ao MinIO — evita transferência em chunks desnecessária e garante integridade do objeto.
- Todas as URLs de dependências configuradas via variável de ambiente — sem SSRF por configuração externa.

### Principais riscos e limitações

| Risco | Severidade | Mitigação atual | Recomendação para produção |
|---|---|---|---|
| PostgreSQL sem TLS | Média | `sslmode=disable` — rede Docker isolada | Habilitar TLS e `sslmode=verify-full` |
| MinIO sem TLS | Média | `MINIO_USE_SSL=false` — rede isolada | Habilitar TLS no MinIO |
| Sem retry em falha de upload MinIO | Baixa | Retorna erro e o processo fica em `RECEBIDO` indefinidamente | Implementar retry com backoff exponencial |
| Sem TTL nos registros de processo | Baixa | Registros acumulam no PostgreSQL | Implementar arquivamento/expiração de registros antigos |

---

### Build

```bash
cd upload-orchestrator
go build ./...
go vet ./...
```
