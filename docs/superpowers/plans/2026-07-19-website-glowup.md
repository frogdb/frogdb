# Website Glow-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restyle frogdb.dev (Astro Starlight + starlight-theme-rapide) into a dark "data report" design: near-black background, Inter typography, huge tight-tracked headlines, eyebrow labels, pill tags, metric tiles, electric-lime accent — with a matching light mode.

**Architecture:** All styling flows through `website/src/styles/custom.css` (loaded after rapide's `@layer rapide`, so un-layered rules always win) plus two new Astro components: a `Hero.astro` override registered via Starlight's `components` config, and a `MetricTiles.astro` used on the homepage. Custom frontmatter fields (`eyebrow`, `pills`) are added via `docsSchema({ extend })`.

**Tech Stack:** Astro 6, Starlight 0.38, starlight-theme-rapide 0.5.2, `@fontsource-variable/inter`, bun.

**Spec:** `docs/superpowers/specs/2026-07-19-website-glowup-design.md`

## Global Constraints

- All work inside `website/` — no Rust changes.
- Copy must be factual, no marketing hype: real claims only ("Redis 8.x compatible", "250+ Redis-compatible commands", counts from generated data).
- Accent: electric lime `#c3f53c` (dark mode); light mode uses darkened lime `#55761a`-range for AA contrast on white. Amber `#e0a43b` for caution asides (dark), `#b07a1a` (light).
- Dark palette: bg `#0a0a0a`, headings `#e8e6e3`, muted `#8a8a8a`, borders `rgba(255,255,255,0.08)`.
- Light mode stays supported; theme toggle stays.
- No `node_modules` patching — overrides live in `custom.css`, config, and registered components only.
- Package manager: `bun` (never npm/npx).
- Build check: `cd website && bun run build` must pass (includes starlight-links-validator).
- Working dir for all paths below: repo root `/Users/nathan/workspace/workspace-3`.

---

### Task 1: Inter font + design tokens (dark + light)

**Files:**
- Modify: `website/package.json` (via `bun add`)
- Modify: `website/astro.config.mjs` (customCss list)
- Modify: `website/src/styles/custom.css` (replace token section)

**Interfaces:**
- Produces: CSS custom props consumed by all later tasks: `--sl-color-*` overrides, `--frog-amber`. Font active site-wide via `--sl-font`.

- [ ] **Step 1: Install Inter**

```bash
cd website && bun add @fontsource-variable/inter
```

- [ ] **Step 2: Register font CSS in astro.config.mjs**

Find the `customCss` line in `website/astro.config.mjs`:

```js
customCss: ['./src/styles/custom.css'],
```

Replace with:

```js
customCss: ['@fontsource-variable/inter', './src/styles/custom.css'],
```

- [ ] **Step 3: Check rapide token names**

Read `website/node_modules/starlight-theme-rapide/styles/theme.css` (path may be `styles/` or package root — `ls` the package). Confirm the exact names of the rapide UI tokens (expected: `--sl-rapide-ui-bg-color`, `--sl-rapide-ui-header-bg-color`, `--sl-rapide-ui-border-color`) and which blocks (`:root`, `[data-theme='light']`) define them. Adjust Step 4's rapide-token lines if names differ.

- [ ] **Step 4: Replace the token section of custom.css**

In `website/src/styles/custom.css`, delete the current blocks:
- `:root { --sl-color-accent-low/accent/accent-high ... }`
- `:root[data-theme='dark'] { --sl-color-bg/bg-nav/bg-sidebar ... }`
- `.expressive-code { --ec-codeBg: #161b22; }`

Insert at the top of the file (after the `/* FrogDB Custom Styles */` comment):

```css
/* ---------------------------------------------------------------------------
 * Design tokens — dark-first "data report" theme.
 * Loaded after rapide's @layer rapide, so these un-layered rules always win.
 * ------------------------------------------------------------------------- */

:root {
	--sl-font: 'Inter Variable';
}

:root[data-theme='dark'] {
	/* Grayscale (white = brightest text, black = page bg) */
	--sl-color-white: #e8e6e3;
	--sl-color-gray-1: #d8d6d3;
	--sl-color-gray-2: #c2c0bd;
	--sl-color-gray-3: #8a8a8a;
	--sl-color-gray-4: #585856;
	--sl-color-gray-5: #2e2e2c;
	--sl-color-gray-6: #161614;
	--sl-color-gray-7: #101010;
	--sl-color-black: #0a0a0a;

	/* Surfaces */
	--sl-color-bg: #0a0a0a;
	--sl-color-bg-nav: #0d0d0c;
	--sl-color-bg-sidebar: #0a0a0a;
	--sl-color-hairline: rgba(255, 255, 255, 0.08);
	--sl-color-hairline-light: rgba(255, 255, 255, 0.08);
	--sl-color-hairline-shade: rgba(255, 255, 255, 0.05);

	/* Electric lime accent (frog-adjacent) */
	--sl-color-accent-low: #2c3a10;
	--sl-color-accent: #8fbe27;
	--sl-color-accent-high: #c3f53c;
	--sl-color-text-accent: #c3f53c;

	/* Caution accent */
	--frog-amber: #e0a43b;

	/* Rapide UI chrome tokens (verify names against theme.css in Step 3) */
	--sl-rapide-ui-bg-color: #0f0f0e;
	--sl-rapide-ui-header-bg-color: rgba(10, 10, 10, 0.85);
	--sl-rapide-ui-border-color: rgba(255, 255, 255, 0.08);
}

:root[data-theme='light'] {
	/* Grayscale flips: white = darkest text, black = page bg */
	--sl-color-white: #1a1a17;
	--sl-color-gray-1: #2e2e2a;
	--sl-color-gray-2: #45453f;
	--sl-color-gray-3: #6f6f68;
	--sl-color-gray-4: #9c9c94;
	--sl-color-gray-5: #dedcd6;
	--sl-color-gray-6: #f3f2ee;
	--sl-color-gray-7: #faf9f6;
	--sl-color-black: #fdfcfa;

	--sl-color-bg: #fdfcfa;
	--sl-color-bg-nav: #faf9f6;
	--sl-color-bg-sidebar: #fdfcfa;
	--sl-color-hairline: rgba(0, 0, 0, 0.1);
	--sl-color-hairline-light: rgba(0, 0, 0, 0.08);
	--sl-color-hairline-shade: rgba(0, 0, 0, 0.05);

	/* Lime darkened for AA contrast on white */
	--sl-color-accent-low: #e9f5c7;
	--sl-color-accent: #55761a;
	--sl-color-accent-high: #3f5a11;
	--sl-color-text-accent: #55761a;

	--frog-amber: #b07a1a;

	--sl-rapide-ui-bg-color: #f6f5f1;
	--sl-rapide-ui-header-bg-color: rgba(253, 252, 250, 0.85);
	--sl-rapide-ui-border-color: rgba(0, 0, 0, 0.08);
}

/* Code block background follows the page surface */
:root[data-theme='dark'] .expressive-code {
	--ec-codeBg: #101010;
}
```

Keep the remaining rules in the file (hero sizing, badge spacing, sidebar-topics, site-title) untouched for now — later tasks handle them.

- [ ] **Step 5: Build to verify**

```bash
cd website && bun run build
```

Expected: build succeeds (links validator included). Warnings about unrelated content are pre-existing; new CSS must not error.

- [ ] **Step 6: Commit**

```bash
git add website/package.json website/bun.lock website/astro.config.mjs website/src/styles/custom.css
git commit -m "feat(website): Inter font + dark-first near-black token palette"
```

---

### Task 2: Typography, card, and aside polish (site-wide CSS)

**Files:**
- Modify: `website/src/styles/custom.css` (append)

**Interfaces:**
- Consumes: tokens from Task 1 (`--sl-color-hairline`, `--frog-amber`, gray scale).
- Produces: utility class `.tabular-nums`; polished `.card` / `.sl-link-card` base used by homepage.

- [ ] **Step 1: Append polish rules to custom.css**

```css
/* ---------------------------------------------------------------------------
 * Typography polish
 * ------------------------------------------------------------------------- */

.sl-markdown-content :is(h1, h2, h3),
h1[data-page-title] {
	letter-spacing: -0.02em;
}

h1[data-page-title] {
	font-weight: 800;
}

.tabular-nums {
	font-variant-numeric: tabular-nums;
}

/* ---------------------------------------------------------------------------
 * Cards: rounded, faint fill, hairline border
 * ------------------------------------------------------------------------- */

.card,
.sl-link-card {
	border-radius: 1rem;
	border: 1px solid var(--sl-color-hairline);
	background-color: var(--sl-color-gray-6);
	transition: border-color 0.15s ease;
}

.sl-link-card:hover,
.sl-link-card:focus-within {
	border-color: var(--sl-color-gray-4);
	background-color: var(--sl-color-gray-6);
}

/* ---------------------------------------------------------------------------
 * Asides: left-border callouts; caution goes amber
 * ------------------------------------------------------------------------- */

.starlight-aside {
	border-radius: 0.5rem;
	border-inline-start-width: 3px;
}

.starlight-aside--caution {
	border-inline-start-color: var(--frog-amber);
}
```

Note: before committing, render a page containing a `:::caution` aside (e.g. any docs page with one — `grep -rn ':::caution' website/src/content | head -1`) and check the rendered class names match `.starlight-aside--caution` (Starlight default). If rapide restyles asides with different hooks (check `base.css` for `--sl-rapide-asides-*`), override those custom props instead for the caution variant.

- [ ] **Step 2: Build to verify**

```bash
cd website && bun run build
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add website/src/styles/custom.css
git commit -m "feat(website): typography tracking, card polish, amber caution asides"
```

---

### Task 3: Hero override component

**Files:**
- Modify: `website/src/content.config.ts` (schema extension)
- Create: `website/src/components/Hero.astro`
- Modify: `website/astro.config.mjs` (register override)
- Modify: `website/src/styles/custom.css` (remove obsolete `.hero` rules)

**Interfaces:**
- Consumes: tokens from Task 1.
- Produces: frontmatter fields `eyebrow: string`, `pills: string[]` usable on any docs page; hero renders `<span class="accent">` inside `hero.title` HTML in lime. Task 4 fills these fields on `index.mdx`.

- [ ] **Step 1: Extend the docs frontmatter schema**

Replace `website/src/content.config.ts` docs collection:

```ts
import { defineCollection, z } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';
import { changelogsLoader } from 'starlight-changelogs/loader';

export const collections = {
	docs: defineCollection({
		loader: docsLoader(),
		schema: docsSchema({
			extend: z.object({
				/** Small uppercase label rendered above the hero headline. */
				eyebrow: z.string().optional(),
				/** Outlined pill tags rendered below the hero lede. */
				pills: z.array(z.string()).optional(),
			}),
		}),
	}),
	changelogs: defineCollection({
		loader: changelogsLoader([
			{
				provider: 'keep-a-changelog',
				base: 'changelog',
				changelog: '../CHANGELOG.md',
				title: 'Changelog',
			},
		]),
	}),
};
```

- [ ] **Step 2: Read the stock Hero for the action-button mapping**

Read `website/node_modules/@astrojs/starlight/components/Hero.astro`. Copy its exact `actions.map(...)` destructuring/LinkButton usage into Step 3 if it differs from what's shown there (Starlight minor versions shuffle `attrs`/`icon` handling).

- [ ] **Step 3: Create the Hero override**

Create `website/src/components/Hero.astro`:

```astro
---
/**
 * Hero override: eyebrow label, large tight-tracked headline, lede, pill tags,
 * actions. `eyebrow`/`pills` come from the docsSchema extension in
 * src/content.config.ts. Only renders on `template: splash` pages.
 */
import { Image } from 'astro:assets';
import { LinkButton } from '@astrojs/starlight/components';

const { data } = Astro.locals.starlightRoute.entry;
const { title = data.title, tagline, image, actions = [] } = data.hero || {};
const eyebrow = data.eyebrow;
const pills = data.pills ?? [];

const logo = image && 'file' in image ? image.file : undefined;
const logoAlt = (image && 'alt' in image && image.alt) || '';
---

<div class="frog-hero">
	{logo && <Image src={logo} width={144} alt={logoAlt} class="frog-hero-logo" />}
	{eyebrow && <p class="frog-eyebrow">{eyebrow}</p>}
	<h1 id="_top" data-page-title set:html={title} />
	{tagline && <p class="frog-lede" set:html={tagline} />}
	{
		pills.length > 0 && (
			<ul class="frog-pills">
				{pills.map((pill) => (
					<li>{pill}</li>
				))}
			</ul>
		)
	}
	{
		actions.length > 0 && (
			<div class="frog-actions">
				{actions.map(
					({ attrs: { class: className, ...attrs } = {}, icon, link: href, text, variant }) => (
						<LinkButton {href} {variant} icon={icon?.name} class:list={[className]} {...attrs}>
							{text}
							{icon?.html && <Fragment set:html={icon.html} />}
						</LinkButton>
					)
				)}
			</div>
		)
	}
</div>

<style>
	.frog-hero {
		display: flex;
		flex-direction: column;
		align-items: flex-start;
		gap: 1.25rem;
		padding-block: clamp(2.5rem, 8vw, 5.5rem) 1.5rem;
		text-align: left;
	}
	.frog-hero-logo {
		width: 4.5rem;
		height: auto;
	}
	.frog-eyebrow {
		margin: 0;
		font-size: 0.75rem;
		font-weight: 600;
		letter-spacing: 0.15em;
		text-transform: uppercase;
		color: var(--sl-color-gray-3);
	}
	.frog-hero h1 {
		margin: 0;
		max-width: 60rem;
		font-size: clamp(2.5rem, 6.5vw, 4.5rem);
		font-weight: 800;
		letter-spacing: -0.02em;
		line-height: 1.05;
		color: var(--sl-color-white);
	}
	/* `hero.title` frontmatter HTML is injected via set:html, so it escapes
	   Astro's scoping — target it with :global. */
	.frog-hero h1 :global(.accent) {
		color: var(--sl-color-accent-high);
	}
	.frog-lede {
		margin: 0;
		max-width: 46rem;
		font-size: clamp(1.05rem, 1.6vw, 1.3rem);
		line-height: 1.6;
		color: var(--sl-color-gray-2);
	}
	.frog-pills {
		display: flex;
		flex-wrap: wrap;
		gap: 0.6rem;
		margin: 0;
		padding: 0;
		list-style: none;
	}
	.frog-pills li {
		border: 1px solid var(--sl-color-hairline);
		border-radius: 999px;
		padding: 0.4rem 1rem;
		font-size: 0.85rem;
		color: var(--sl-color-gray-2);
	}
	.frog-actions {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem;
		margin-top: 0.5rem;
	}
</style>
```

- [ ] **Step 4: Register the override**

In `website/astro.config.mjs`, inside the `starlight({ ... })` options (sibling of `customCss`), add:

```js
components: {
	Hero: './src/components/Hero.astro',
},
```

- [ ] **Step 5: Drop obsolete `.hero` rules from custom.css**

Delete from `website/src/styles/custom.css`:

```css
/* Hero section customization */
.hero {
	padding-block: 3rem;
}

/* Hero image sizing */
.hero > img {
	width: min(100%, 22.5rem);
}
```

- [ ] **Step 6: Build to verify**

```bash
cd website && bun run build
```

Expected: PASS. The homepage still renders with old frontmatter (no eyebrow/pills yet — fields are optional).

- [ ] **Step 7: Commit**

```bash
git add website/src/content.config.ts website/src/components/Hero.astro website/astro.config.mjs website/src/styles/custom.css
git commit -m "feat(website): custom hero override with eyebrow, pills, huge headline"
```

---

### Task 4: MetricTiles + homepage restructure

**Files:**
- Create: `website/src/components/MetricTiles.astro`
- Modify: `website/src/content/docs/index.mdx`

**Interfaces:**
- Consumes: `eyebrow`/`pills` frontmatter (Task 3), card polish (Task 2), `.tabular-nums` semantics via component-local styles, `src/data/compat-exclusions.json` (`summary.total_tests: number`, `summary.upstream_version: string` e.g. `"Redis 8.6.0"`).

- [ ] **Step 1: Create MetricTiles**

Create `website/src/components/MetricTiles.astro`:

```astro
---
/**
 * Homepage metric tiles. Dynamic numbers come from the auto-generated Redis
 * test-suite data (just compat-gen); static ones mirror claims already made
 * in the docs (commands.mdx, index feature list).
 */
import data from '../data/compat-exclusions.json';

const { total_tests, upstream_version } = data.summary;

const tiles = [
	{ value: '250+', label: 'Redis-compatible commands' },
	{ value: total_tests.toLocaleString('en-US'), label: 'ported Redis test cases' },
	{ value: upstream_version.replace('Redis ', ''), label: 'upstream Redis target' },
	{ value: '10', label: 'data structure families' },
];
---

<div class="frog-metrics">
	{
		tiles.map((tile) => (
			<div class="frog-metric">
				<span class="frog-metric-value">{tile.value}</span>
				<span class="frog-metric-label">{tile.label}</span>
			</div>
		))
	}
</div>

<style>
	.frog-metrics {
		display: grid;
		grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr));
		gap: 1rem;
		margin-block: 2rem;
	}
	.frog-metric {
		display: flex;
		flex-direction: column;
		gap: 0.35rem;
		padding: 1.5rem 1.25rem;
		border: 1px solid var(--sl-color-hairline);
		border-radius: 1rem;
		background-color: var(--sl-color-gray-6);
	}
	.frog-metric-value {
		font-size: clamp(1.75rem, 3vw, 2.5rem);
		font-weight: 800;
		letter-spacing: -0.02em;
		line-height: 1.1;
		font-variant-numeric: tabular-nums;
		color: var(--sl-color-white);
	}
	.frog-metric-label {
		font-size: 0.85rem;
		color: var(--sl-color-gray-3);
	}
</style>
```

- [ ] **Step 2: Rewrite index.mdx frontmatter + add tiles**

Replace the frontmatter of `website/src/content/docs/index.mdx` with:

```yaml
---
title: FrogDB
description: A Redis 8.x-compatible, memory-first database built in Rust
template: splash
eyebrow: 'FrogDB · Memory-first database · Built in Rust'
pills:
  - RESP2 / RESP3
  - Single binary
  - RocksDB persistence
  - Raft clustering
  - Lua scripting
hero:
  image:
    file: ../../assets/frogdb-logo.png
    alt: FrogDB Logo
  title: '<span class="accent">Redis 8.x</span> compatible. Memory-first. Built in Rust.'
  tagline: FrogDB implements the Redis 8.x command surface — core types, JSON, time series, vector sets, search, and more — on a memory-first Rust core with RocksDB-backed persistence, Raft clustering, and replication.
  actions:
    - text: Get Started
      link: /getting-started/quickstart/
      icon: right-arrow
      variant: primary
    - text: View on GitHub
      link: https://github.com/frogdb/frogdb
      icon: external
---
```

Then add the import and place tiles at the top of the body (before `## Features`):

```mdx
import { CardGrid, LinkCard } from '@astrojs/starlight/components';
import Card from 'starlight-plugin-icons/components/Card.astro';
import MetricTiles from '../../components/MetricTiles.astro';

<MetricTiles />

## Features
```

Keep the rest of the body (Features cards, data structures list, quick example, next steps) unchanged.

- [ ] **Step 3: Build to verify**

```bash
cd website && bun run build
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add website/src/components/MetricTiles.astro website/src/content/docs/index.mdx
git commit -m "feat(website): homepage metric tiles and factual hero copy"
```

---

### Task 5: Visual verification (main session, not subagent)

**Files:** none created; fixes fold back into the files above.

- [ ] **Step 1: Start dev server**

```bash
just docs-dev
```

(Runs docs-gen + compat-gen, then `bun run dev` — serves at http://localhost:4321.)

- [ ] **Step 2: Screenshot review (Chrome tools, main session)**

Screenshot and inspect against reference screenshots + spec:
1. Homepage, dark mode (hero, tiles, cards).
2. Homepage, light mode (toggle theme).
3. One docs page dark (e.g. /getting-started/quickstart/) — sidebar, code blocks, asides.
4. Compatibility page (CompatSummary stat cards should not clash with new tokens).

Checks: eyebrow tracking/size, headline weight/tracking/clamp behavior at narrow width, lime contrast on black, dark-lime contrast on white, card borders visible but faint, header/sidebar chrome matches near-black, EC code blocks not lighter than page.

- [ ] **Step 3: Iterate with user**

Present screenshots; apply requested tweaks; re-screenshot.

- [ ] **Step 4: Final build + commit any fixes**

```bash
cd website && bun run build
git add -A website/src && git commit -m "fix(website): visual polish from screenshot review"
```

(Skip commit if no fixes.)
