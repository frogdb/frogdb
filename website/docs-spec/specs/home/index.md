# Spec: index.mdx
Status: rewrite
Audiences: A1 (primary), A2, A3 — a landing page that orients every arriving reader in ~30 seconds and routes them to the right section.

Goal: In under 30 seconds the reader learns (1) what FrogDB is — an in-memory database written in Rust that implements RESP2/RESP3 and targets compatibility with Redis 8.x, with RocksDB-backed persistence, shipped as a single binary; (2) that it is pre-release, unreleased software; and (3) where to go next — Quickstart, the compatibility/correctness evidence, and the architecture. Every substantive claim on the page links to the page that substantiates it. The page reads as sober engineering documentation, not marketing.

Not in scope:
- Visual/CSS design. The splash `template` + `Hero.astro` own all styling; recent commits (d60335eb, 628a1f5f, 19c47baa, c309eaa4, 5fedbd9b) already reworked the homepage look. Write content only: frontmatter fields, prose, card text, links.
- Any per-command reference, config tables, or deep feature docs — those live in their sections.
- Performance/throughput/latency numbers of any kind (no benchmarks are published; PLAN §6 policy).
- Hardcoded version numbers, command counts, or crate counts anywhere in prose (PLAN §6). If a version must appear, it comes from `versions.json` (S6); otherwise omit it.

Sources of truth:
- `website/src/content/docs/index.mdx` — current page (being replaced).
- `website/src/components/Hero.astro` — consumes `hero.title` (HTML via `set:html`, so `<span class="accent">` is allowed), `hero.tagline` (Markdown), `hero.image`, `hero.actions`. Do not add hero fields the component does not read.
- `website/docs-spec/PLAN.md` — §4 (hype to cut), §5 Home goal, §7 style rules.
- `.github/workflows/release.yml` — the published image names are `docker.io/frogdb/frogdb` and `ghcr.io/nathanjordan/frogdb`, and the repo owner is `nathanjordan` (resolved in operations/deployment.md); the "View on GitHub" URL is `https://github.com/nathanjordan/frogdb`.

Existing content: Replace `website/src/content/docs/index.mdx` wholesale. Mine it for the true skeleton (Features cards, data-structure list, Testing summary, Quick Example, Next Steps) but rewrite every claim to be grounded and linked.

Structure:

- **Frontmatter (`hero`)**
  - `title`: a grounded one-liner. Keep the form "Redis 8.x compatible. Memory-first. Built in Rust." (accurate: RESP2/RESP3, in-memory, Rust). One `<span class="accent">` highlight is fine.
  - `tagline`: state plainly what FrogDB is and that it is pre-release. Cut the current tagline's unbacked "Extensively tested for Redis compatibility and correctness." Replace with a factual sentence, e.g. that it implements RESP2/RESP3, targets Redis 8.x commands, supports replication and cluster mode, and offers RocksDB-backed persistence — with the correctness/testing claim moved to the Testing section where it links to evidence. Include a pre-release honesty note (either here or in an `Aside` immediately below the hero).
  - `actions`: "Get Started" → Quickstart slug; "View on GitHub" → `https://github.com/nathanjordan/frogdb` (owner `nathanjordan`, resolved in operations/deployment.md from `release.yml`). The current page's `https://github.com/frogdb/frogdb` is wrong.

- **Pre-release status note** (one short `Aside` or sentence): FrogDB is unreleased, pre-production software. This is required by PLAN §5 ("its status (pre-release)"). Place it high — hero tagline or an Aside directly under it.

- **## Features** (CardGrid of ~4–6 cards). Each card is one factual capability with a link to its home page. No absolutes ("Full support", "all types") unless the linked page substantiates them.
  - Redis compatibility card → links to Compatibility & Correctness overview/differences and the generated command matrix. Phrase as "targets compatibility with Redis 8.x commands over RESP2/RESP3; wire-compatible with existing Redis clients" — not "Full support for all Redis types".
  - Clustering & Replication card → links to Operations → Clustering and Replication. Note cluster metadata is Raft-coordinated (link Architecture → Clustering).
  - Persistence card → links to Operations → Persistence & durability. "RocksDB-backed persistence with configurable durability modes; optional tiered hot/warm storage." Confirm tiered-storage is a real feature (`[tiered-storage] enabled = false` exists in config) — present it as optional/off-by-default, link to the persistence page.
  - Observability/Operations card → links to Operations → Observability and Diagnostics. List only shipped surfaces: Prometheus metrics, OTLP export, HTTP debug UI, diagnostic bundles, management CLIs (`frog`/`frogdb-admin`). CUT "Kube operator coming soon" (PLAN §6: no coming-soon; Kubernetes page is cut per §4).
  - Testing/correctness card → links to Compatibility & Correctness → Testing methodology. Replace "Extensively tested" with specifics that the linked page backs up.
  - "Built with Rust" card → keep factual (memory-safe, single binary). CUT "and maybe a neck beard" (PLAN §4).

- **## Data structures / feature families** (rewrite of "Redis 8.x Data Structures"). Do NOT claim "supports the full Redis 8.x feature set" as an unqualified absolute. Split honestly:
  - Core Redis types (Strings, Lists, Sets, Sorted Sets, Hashes, Streams, Bitmaps/Bitfields, Geospatial, Pub/Sub, Scripting, Transactions) → link to the command matrix for exact per-command status.
  - Extension families (JSON, Time Series, Search FT.*, probabilistic BF/CF/CMS/TOPK/TD, Vector Sets, Event Sourcing ES.*) → link to the Extensions overview. Note these are Redis-Stack-compatible except ES.* which is FrogDB-original. Do not present extensions as "core Redis 8.x".
  - This section is a scannable list with links, not a compatibility table (the matrix is the table).

- **## Testing** (short). Summarize the testing pyramid in one paragraph + a short list, each item grounded, and link to Testing methodology for the full story. Include only what exists: unit/integration, Shuttle randomized concurrency, Turmoil network simulation, Jepsen (Knossos/Elle) replication + Raft cluster testing, cargo-fuzz targets, ported Redis TCL regression suites, history-based consistency checking. Do NOT mention Loom (no loom dependency exists — PLAN §4). Do NOT put a hardcoded fuzz-target count on the homepage; link instead.

- **## Quick Example** (keep). A minimal `frogdb-server` start + `redis-cli` connect + one or two commands. Must match the Quickstart page exactly (same binary name `frogdb-server`, same `redis-cli` invocation). Keep it tiny; the Quickstart is the real walkthrough.

- **## Next Steps** (CardGrid of LinkCards). Route to the canonical new-IA pages: Installation, Quickstart, Compatibility & Correctness (overview/differences), Architecture overview. VERIFY-BEFORE-WRITING: exact slugs against the final sidebar/IA — the current page links `/reference/reference-config/`, `/compatibility/redis-differences/`, and `/guides/event-sourcing/`, all of which move or are renamed in this restructure (§4 dissolves `guides/`; Configuration reference and differences pages are renamed). Do not emit a link to a page that does not exist in the new IA.

Generated data:
- `versions.json` (S6) — the only sanctioned source if any version string appears in prose. Preferred: omit versions from the homepage entirely and let sub-pages surface them.
- No other generated JSON is consumed directly here; the homepage links to the pages that render `commands.json` (S1/S2), `compat-exclusions.json`, `config-reference.json`, etc.

Drift guards:
- Every claim links to its substantiating page, so a reader (and reviewer) can check it; unlinkable claims are cut.
- No hardcoded version/command/crate counts (PLAN §6) — nothing to drift.
- Link targets should be validated by the site's link checker (`just docs-link-check`) so renamed/removed IA pages fail the build rather than rot silently.
- Capability cards name only shipped features; anything partial or planned is omitted, not labeled "coming soon".
