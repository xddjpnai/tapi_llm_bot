# Multi-Agent Cluster Architecture

This document outlines how to evolve the existing investment assistant into a modular, multi-cluster platform that can be shared with other users. The plan assumes n8n will serve as the orchestration layer, while PostgreSQL persists state across clusters.

---

## Vision Overview

- Clusters represent life domains (e.g., **Finance**, **Learning**, **Business**). Each cluster bundles integrations, storage schema, workflows, and assistant logic.
- A single Telegram entry point routes users to clusters they own or subscribe to.
- n8n workflows act as “agents”. Each agent is parameterised by `cluster_instance_id`, providing per-user or multi-tenant isolation.
- Shared services (scheduler, tokens, notifications, LLM gateway) provide common capabilities for all clusters.

---

## Shared Infrastructure

- **n8n workspace**
  - Folder structure per cluster (`Finance`, `Learning`, etc.).
  - Reusable workflows for common tasks (token fetch, user lookup, notification dispatch).
- **PostgreSQL**
  - Serves as the primary state store for users, cluster instances, portfolio data, schedules, and audit logs.
  - Sensitive fields (Tinkoff tokens, API secrets) are encrypted at rest (PGP/GCP KMS/Vault).
- **Secrets Management**
  - Global credentials (Telegram Bot, Perplexity) are stored as n8n credentials.
  - User-provided credentials (Tinkoff tokens) reside in PostgreSQL with encryption and consent tracking.
- **Messaging**
  - Kafka remains available for high-volume events. n8n workflows can subscribe via Kafka Trigger or HTTP callbacks.
  - For simpler flows, HTTP triggers and internal workflow calls suffice.
- **LLM Gateway**
  - A dedicated workflow handling Perplexity/OpenAI calls: request throttling, caching, logging, fallbacks to classical web search.

---

## Data Model (ER Outline)

| Table | Purpose | Key Columns |
| --- | --- | --- |
| `users` | Master list of platform users. | `id`, `telegram_id`, `display_name`, `timezone`, `language`, `created_at`, `updated_at` |
| `cluster_definitions` | Catalog of cluster templates. | `id`, `slug`, `version`, `title`, `description`, `price_plan`, `is_public` |
| `cluster_versions` | Version history per cluster template. | `id`, `cluster_definition_id`, `version`, `workflow_export`, `schema_migration`, `changelog`, `released_at` |
| `cluster_instances` | Concrete cluster deployments per user/subscriber. | `id`, `cluster_definition_id`, `owner_user_id`, `subscriber_user_id`, `status`, `params`, `created_at`, `expires_at` |
| `cluster_entitlements` | Access rights for users. | `id`, `cluster_instance_id`, `user_id`, `role` (`owner`, `subscriber`, `admin`), `status`, `start_at`, `end_at` |
| `tinkoff_tokens` | User Tinkoff API tokens. | `owner_user_id`, `subscriber_user_id`, `token`, `scopes`, `rotated_at` |
| `telegram_sessions` | Chat context, last commands per user. | `user_id`, `chat_id`, `state`, `payload`, `updated_at` |
| `portfolio_positions` | Aggregated portfolio data per cluster instance. | `cluster_instance_id`, `ticker`, `figi`, `quantity`, `expected_yield`, `expected_yield_percent`, `market` |
| `cluster_settings` | Per-cluster settings (daily summary time, notifications, etc.). | `cluster_instance_id`, `key`, `value` |
| `jobs` | Scheduled jobs produced by the scheduler. | `id`, `cluster_instance_id`, `job_type`, `payload`, `run_at`, `status` |
| `llm_requests` | Audit log of LLM usage for billing/diagnostics. | `id`, `cluster_instance_id`, `user_id`, `provider`, `prompt`, `tokens_in`, `tokens_out`, `cost`, `status`, `created_at` |
| `events` | Unified event log (user actions, cluster operations). | `id`, `cluster_instance_id`, `type`, `payload`, `created_at` |

**Relationships**

- `cluster_definitions` (1) → `cluster_versions` (many)
- `cluster_definitions` (1) → `cluster_instances` (many)
- `cluster_instances` (1) → `cluster_entitlements` (many)
- `cluster_instances` (1) → `portfolio_positions` / `cluster_settings` / `jobs` / `llm_requests` / `events`
- `users` (1) → `cluster_instances` (many via `owner_user_id` / `subscriber_user_id`)
- `users` (1) → `cluster_entitlements` (many)
- `users` (1) → `tinkoff_tokens` (many; tokens may belong solely to owner)

> **Security Note**: `tinkoff_tokens.token` should be stored encrypted. Store encryption metadata separately (`encryption_info` table) if using external KMS.

---

## Workflow Lifecycle

1. **Cluster Definition / Update**
   - Author designs or updates a cluster workflow in n8n.
   - Workflow export JSON + schema migration stored in `cluster_versions`.
   - Release action marks version as active; migrations applied to existing instances.

2. **Subscription / Provisioning**
   - Payment webhook (Stripe/YooKassa) triggers workflow `activate-cluster`.
   - Creates `cluster_instance` with `status = active`, writes `cluster_entitlement` for subscriber, requests required tokens.
   - Sends onboarding flow via Telegram (prompts for Tinkoff token, configuration).

3. **Runtime Execution**
   - Telegram commands route through `telegram-router` workflow: determine active cluster, contextualise message (`cluster_instance_id`, `role`).
   - Agent workflows execute domain logic (e.g., `finance-portfolio-sync`, `finance-daily-summary`), read/write data rows filtered by `cluster_instance_id`.
   - Scheduler workflow reads `jobs` and `cluster_settings` to dispatch timed tasks.
   - Notifications go through `notification-dispatcher`, formatting UI and sending via Telegram or email.

4. **Monitoring & Billing**
   - LLM gateway logs each request to `llm_requests` with cost metrics.
   - Event logger persists state changes (token updates, portfolio sync, errors).
   - Billing workflow aggregates metrics per subscriber for invoicing.

5. **Termination**
   - Subscription expiry cron marks `cluster_entitlement.status = expired`, disables jobs, notifies user.
   - Data retention policy optionally archives or deletes `portfolio_positions` / `events` after grace period.

---

## Shared Services (Workflow Catalog)

- **telegram-router**: Entry point for all messages. Determines `user_id`, looks up entitlements, routes to cluster-specific sub-workflows, and maintains session state.
- **profile-service**: CRUD operations for `users`, `cluster_entitlements`, and Telegram session context.
- **scheduler**: Cron-triggered workflow that reads `jobs` table and invokes cluster workflows when `run_at <= now()`.
- **notification-dispatcher**: Formats and sends messages (Telegram, email, push); handles inline keyboards and alert routing.
- **llm-gateway**: Handles Perplexity/OpenAI requests, fallbacks to web search, enforces rate limits, and logs usage to `llm_requests`.
- **token-service**: Receives tokens from users, validates them (optional live check against Tinkoff), encrypts, stores, and tracks rotation time.
- **cluster-manager**: Automates lifecycle tasks (provisioning, upgrades, termination, backup, cloning).

---

## Finance Cluster (Current Baseline)

- **Gateway workflows**: Tinkoff portfolio fetch, instrument lookup, quote retrieval, chart generation.
- **Assistants**: Summary generation (Perplexity), news aggregation (Perplexity + fallback links), daily digest scheduler.
- **Storage**: Uses `portfolio_positions`, `cluster_settings`, `llm_requests` and shared notifications.
- **Outstanding fixes**:
  - Repair daily summary pipeline (scheduler → summary-service → notifications).
  - Improve news workflow (Perplexity error handling, Russian market coverage).
  - Add RU market instrument metadata and localisation.

This cluster will serve as the template for future domains.

---

## Roadmap / Next Steps

1. **Stabilise Finance Cluster**
   - Debug daily summary & news workflows.
   - Add Russian market instrument handling and localisation.
   - Clean up Kafka producers/consumers for better observability.

2. **Implement Shared Services**
   - Stand up PostgreSQL schema with new multi-tenant tables.
   - Build token service + encryption.
   - Launch LLM gateway workflow with usage logging.
   - Deploy notification dispatcher and scheduler.

3. **Define Cluster Template Framework**
   - Export baseline finance workflows into `cluster_versions`.
   - Automate provisioning (activate/deactivate clusters).
   - Create CLI or n8n API scripts to clone workflows per cluster instance.

4. **Enable Monetisation**
   - Integrate payment provider webhook.
   - Implement entitlement management and billing reports.
   - Build Telegram onboarding for subscribers.

5. **Launch Additional Clusters**
   - Learning/Self-development (LMS integrations, study planner).
   - Business operations (CRM tasks, KPIs, reminders).
   - Cross-cluster digest aggregator (“Life overview” daily summary).

6. **Operational Hardening**
   - Observability dashboards (workflow execution metrics, error alerts).
   - Data retention & compliance policies for shared clusters.
   - Disaster recovery procedures for n8n PostgreSQL and workflow exports.

---

## Contribution Notes

- Keep documentation in this README up to date when clusters, shared services, or schemas evolve.
- When adding a new cluster, create complementary docs under `docs/` (workflow diagrams, API references).
- Enforce security practices for handling third-party tokens and personal data.


