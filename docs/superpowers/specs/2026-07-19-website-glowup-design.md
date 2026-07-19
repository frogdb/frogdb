# Website Glow-Up: Dark "Data Report" Redesign

**Date:** 2026-07-19
**Scope:** `website/` (Astro Starlight + starlight-theme-rapide)
**Status:** Approved

## Goal

Restyle frogdb.dev to match a dark "data report" design system: near-black background,
bold Inter typography with huge tight-tracked headlines, uppercase eyebrow labels,
rounded faint-bordered cards, metric tiles, and an electric-lime frog-adjacent accent.
The current site (stock rapide theme, splash homepage) reads as mechanical; the homepage
is the priority, but tokens apply site-wide so docs pages stay coherent.

Copy stays factual. No marketing hype, no underselling. Claims must be true and
concrete (e.g. "Redis 8.x compatible", real command/test counts).

## Decisions (user-approved)

- **Scope:** Full — site-wide tokens + fonts + card polish, custom Hero override,
  redesigned homepage sections.
- **Accent:** Electric lime, frog-adjacent (~`#c3f53c`, tuned to keep frog identity).
  Single accent. Amber `#e0a43b` for caution callouts.
- **Light mode:** Kept. Dark-first design; light mode gets a matching tuned palette
  (paper-white bg, lime darkened for AA contrast). Theme toggle stays.
- **Copy:** Factual. Example headline direction: "FrogDB. A memory-first database,
  Redis 8.x compatible." Final wording iterated during build.

## Current State (surveyed)

- `astro.config.mjs`: Starlight 0.38 + `starlight-theme-rapide` 0.5.2, UnoCSS
  (icons only), `customCss: ['./src/styles/custom.css']`, no `components:` overrides.
- `src/styles/custom.css`: only overrides 3 accent tokens (green `#6BAA3D` family),
  3 dark bg tokens (`#0d1117`), hero padding, sidebar-topics tweaks.
- No custom fonts anywhere; `--sl-font` unset (system stack).
- Homepage: `src/content/docs/index.mdx`, `template: splash`, stock `Hero.astro`,
  stock `CardGrid`/`Card`/`LinkCard` sections.
- Rapide defines full OKLCH palette in `@layer rapide`; our un-layered `custom.css`
  loads after and wins on any clash.
- Real stats available in `src/data/` (compat suite JSON) + `CompatSummary.astro`
  stat-card pattern.

## Design

### 1. Tokens + typography (site-wide)

- Add `@fontsource-variable/inter` (bun). Import in `custom.css`. Set
  `--sl-font: 'Inter Variable'`. Mono stays rapide/system.
- Dark palette: `--sl-color-bg: #0a0a0a`; nav/sidebar near-black; primary text
  `#e8e6e3`; muted `#8a8a8a`; hairlines/borders `rgba(255,255,255,0.08)`.
  Map onto `--sl-color-gray-*` scale so all rapide components inherit.
- Accent triad: `--sl-color-accent-high` ≈ `#c3f53c`, mid and low tuned darker
  lime-green. Verify link/button contrast on near-black.
- Amber `#e0a43b` wired into caution-type asides (rapide `--sl-rapide-asides-*` tokens).
- Light mode block: paper-white bg, dark-lime accent (~`#5a7d1a` range, AA on white),
  matching gray scale.
- Headings: `letter-spacing: -0.02em`, h1 weight 800. `font-variant-numeric:
  tabular-nums` for stat/metric numerals.

### 2. Card polish (site-wide)

- `.card`, `.sl-link-card`: border-radius 16px, 1px `rgba(255,255,255,0.08)` border,
  faint fill (slightly lighter than bg), subtle hover border lift.
- Optional accent top-border variant (colored 2–3px top edge) for lead cards.
- Asides: left-border callout style; caution uses amber.

### 3. Hero override

- New `src/components/Hero.astro`; register via
  `starlight({ components: { Hero: './src/components/Hero.astro' } })`.
- Structure: eyebrow (uppercase, 12px, 0.15em tracking, muted) → headline
  (`clamp(2.5rem, 7vw, 4.5rem)`, weight 800, -0.02em, "FrogDB" in accent) →
  factual lede paragraph → outlined pill tags ("Redis 8.x protocol", "RESP3",
  "Single binary", "Rust") → action buttons (existing frontmatter `actions`).
- Logo: small mark near eyebrow or omitted from hero (header keeps it) — decided
  visually during build.
- Only affects `template: splash` pages; docs pages untouched.

### 4. Homepage restructure (`index.mdx`)

- Eyebrow: "REDIS 8.x COMPATIBLE · MEMORY-FIRST · RUST" (or similar factual line).
- Metric tiles row: new `MetricTiles.astro` (adapting `CompatSummary` pattern),
  big tabular numerals + small muted captions, sourced from `src/data/` compat JSON —
  commands implemented, test-suite pass rate, data structure count.
- Feature cards: keep 6 factual features, restyled grid.
- Quick example: keep bash code block, framed consistently.
- Next steps: `LinkCard`s inherit card polish.

## Error handling / edge cases

- Rapide updates could reshuffle tokens: all overrides live in `custom.css` +
  registered components; no `node_modules` patching.
- Frontmatter schema: Starlight hero schema has no eyebrow/pills fields — those live
  in the Hero component or via `hero.title`/`tagline` reuse; avoid schema hacks.
- Fonts: variable font single-file; subset via fontsource defaults; check build size.

## Verification

- `just docs-dev` → screenshot homepage, one docs page, light + dark (Chrome tools);
  compare against reference screenshots.
- `just docs-build` (runs links validator) must pass.
- Contrast spot-check: lime-on-black and dark-lime-on-white for text-sized usage.

## Implementation notes

- Work happens in `website/` only; no Rust changes.
- Delegate build phases to subagents (Opus/Sonnet); screenshot checkpoints with user.
