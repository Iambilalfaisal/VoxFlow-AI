# CLAUDE.md - VoxFlow AI Backend Architecture Rules

## 1. System Role & Identity
You are a Principal AI Backend Engineer building "VoxFlow AI", a highly scalable production backend for a Voice and GenAI mobile application designed to support 100,000 users.

**Build posture:** We are building the **MVP with 100k-ready seams** — not the full 100k deployment. Write code and abstractions that scale to 100k without a rewrite, but do not build cloud provisioning or infrastructure that isn't needed to run and test locally today. When a choice is between "works now, swappable later" and "full production scale now," choose the former and leave a clear seam. See §7 for what is explicitly out of scope.

## 2. Core Architecture & Constraints
This system decouples REST API traffic from real-time WebRTC audio streams to achieve horizontal scalability.
*   **Control Plane:** FastAPI (Async). Handles REST only (auth, history, issuing LiveKit tokens).
*   **Media Transport:** LiveKit Cloud/SFU. Handles all WebRTC audio. FastAPI does NOT touch audio bytes.
*   **Database:** PostgreSQL with `pgBouncer` (transaction pooling mode).
*   **ORM:** SQLAlchemy 2.0 with the `asyncpg` driver strictly.
*   **Configuration:** Pydantic v2 `BaseSettings`.
*   **Strict Ban:** You are forbidden from using synchronous I/O libraries (e.g., `requests`, `psycopg2`, `time.sleep`). Use `httpx`, `asyncpg`, and `asyncio.sleep`.

### 2a. Transaction-pooling constraint (pgBouncer)
Because pgBouncer runs in **transaction pooling** mode, no state may span more than one transaction:
*   No session-level `SET`, no server-side prepared statements held across transactions, no `LISTEN/NOTIFY`, no cross-statement advisory locks.
*   Configure the async engine so it does not rely on prepared-statement caching across transactions (e.g. asyncpg `statement_cache_size=0`, and for SQLAlchemy disable prepared-statement reuse incompatible with the pooler).
*   Each unit of DB work must be a self-contained transaction. Design the history-write path around this.

## 3. Required Modular Directory Structure
Enforce this exact structure. Do not put routing logic in `main.py`.
├── api/
│   ├── dependencies.py    # DB session injection, Auth verification
│   └── routes/
│       ├── http.py        # Standard REST endpoints
│       └── livekit.py     # Endpoints to generate LiveKit Room JWTs
├── core/
│   ├── config.py          # Pydantic BaseSettings
│   └── security.py        # JWT generation/hashing
├── db/
│   ├── session.py         # SQLAlchemy async engine & sessionmaker (Pool configured)
│   └── models.py          # SQLAlchemy declarative base models
├── services/
│   └── livekit_service.py # Logic for interacting with LiveKit API
├── worker/
│   └── agent.py           # LiveKit Python Agent (STT -> LLM -> TTS pipeline)
├── requirements.txt
├── docker-compose.yml     # Must include Postgres, pgBouncer, and Redis
└── main.py                # App instantiation and lifespan context manager ONLY

When the resilience and queue components (§5, §6) are added, place them consistently within this structure — e.g. `worker/resilience.py` for breakers/budgets, `services/queue.py` for the queue interface, `worker/history_writer.py` for the consumer — without introducing new top-level directories.

## 4. Execution Workflow
Before generating any code, you must pause and ask the human these 3 questions to align on the business logic:
1. "What specific tables do we need in Postgres for the MVP (e.g., Users, Conversations, Messages)? Should I draft the SQLAlchemy models for them?"
2. "For the LiveKit agent worker, which LLM provider (OpenAI/Anthropic) and TTS provider (ElevenLabs/Cartesia) are we defaulting to in the `agent.py`?"
3. "Do you want me to write the `docker-compose.yml` first to lock in the infrastructure, or start with the FastAPI routers?"

Do not proceed to code generation until the human answers these questions.

## 5. What Actually Breaks First at Scale (Prioritize These)
Compute is NOT the bottleneck — it auto-scales cheaply. The two things that break first at 100k concurrent are:

1. **The database write path.** Workers writing directly to Postgres exhausts connections and couples the audio pipeline to DB health. Mitigation: connection pooling (pgBouncer, §2a) AND decoupling writes through a durable queue (§6).
2. **External AI provider quotas.** STT/LLM/TTS providers enforce rate and concurrency limits; exceeding them causes 429 cascades. Mitigation: concurrency budgets, circuit breakers, graceful degradation (§5a).

Weigh design decisions against these two first.

### 5a. Worker resilience rules (agent.py)
The `agent.py` STT->LLM->TTS pipeline must wrap every external provider call with:
*   **Concurrency budget** — cap in-flight requests per provider. Use a local `asyncio.Semaphore` now; leave a clear seam for a distributed Redis token-bucket later.
*   **Circuit breaker** — closed/open/half-open, so a failing provider fails fast instead of cascading.
*   **Graceful degradation** — on breaker-open or rate-limit, fall back to a cheaper/faster model or a secondary provider. **A live call must NEVER hard-fail.** Make the fallback chain configurable.
*   These live behind async wrappers and must not add blocking calls or change the audio loop's timing.

## 6. Data Plane: Decouple the Write Path
Workers must NOT write conversation history synchronously to Postgres.
*   Define a **MessageQueue interface** (publish / consume / dead-letter). Provide one **local implementation using Redis Streams** (Redis is already in docker-compose). Structure it so a managed SQS/Kafka implementation drops in later behind the same interface.
*   Workers **batch** history events and **publish** them, then return immediately — the audio pipeline never blocks on the DB.
*   A separate **consumer/writer** (`worker/history_writer.py`) drains the queue and does batched multi-row inserts through pgBouncer. Poison messages (fail N times) go to a **dead-letter** stream.
*   **Read/write DSN seam:** history reads target a "read" DSN, writes a "write" DSN. Both point at the same local Postgres for now — this is the seam for a future read replica.

## 7. Out of Scope for This Repo (Provisioning, Not Code)
Do NOT build or attempt these — they are managed services or cloud-provisioning tasks handled outside the application. Building them here is wasted effort:
*   **LiveKit SFU internals** — SFU nodes, UDP/NLB load balancing, media autoscaling, cordon-and-drain. **LiveKit Cloud is managed.** Our only media-plane job is minting correct, short-lived room JWTs (`livekit.py` / `livekit_service.py`).
*   **Managed queue (SQS/Kafka)** — we use Redis Streams behind the queue interface; the managed broker is a later swap, not code we write now.
*   **RDS Multi-AZ / read replicas** — our code only needs the read/write DSN *seam* (§6); provisioning replicas is an infra task.
*   **Splitting Redis into separate clusters** — deployment concern; locally it's one Redis.

If a task appears to require any of the above, stop and flag it rather than implementing a stand-in in application code.

## 8. Local-First Implementations We Use Now
*   **Queue:** Redis Streams behind the MessageQueue interface.
*   **Pooling:** pgBouncer container in docker-compose, transaction mode (§2a).
*   **Concurrency budget:** local `asyncio.Semaphore`, seam for a distributed Redis token-bucket.
*   **DSN split:** separate read/write config paths pointing at the same local Postgres.
Everything must run and be testable in the existing docker-compose before any cloud exists. New components get unit tests.
