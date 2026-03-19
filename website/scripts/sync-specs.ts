#!/usr/bin/env bun
/**
 * Syncs docs/{contributors,operators,users}/ markdown files into
 * website/src/content/docs/{architecture,operations,guides}/ with
 * Starlight-compatible frontmatter and transformed internal links.
 *
 * Usage:
 *   bun run scripts/sync-specs.ts          # one-shot sync
 *   bun run scripts/sync-specs.ts --watch  # sync + watch for changes
 */

import { readdir, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { join, basename } from "node:path";
import { watch } from "node:fs";

const ROOT = join(import.meta.dir, "../..");
const WEBSITE = join(import.meta.dir, "..");

/** Each source directory maps to a website output directory and URL prefix. */
interface SyncSection {
  sourceDir: string;
  outDir: string;
  urlPrefix: string;
}

const SECTIONS: SyncSection[] = [
  {
    sourceDir: join(ROOT, "docs/contributors"),
    outDir: join(WEBSITE, "src/content/docs/architecture"),
    urlPrefix: "/architecture",
  },
  {
    sourceDir: join(ROOT, "docs/operators"),
    outDir: join(WEBSITE, "src/content/docs/operations"),
    urlPrefix: "/operations",
  },
  {
    sourceDir: join(ROOT, "docs/users"),
    outDir: join(WEBSITE, "src/content/docs/guides"),
    urlPrefix: "/guides",
  },
];

/** Map from source dirname to URL prefix for cross-directory link resolution. */
const DIR_TO_PREFIX: Record<string, string> = {
  contributors: "/architecture",
  operators: "/operations",
  users: "/guides",
};

// Sidebar ordering per section (keys are filename stems without .md)
const ORDER_MAP: Record<string, Record<string, number>> = {
  contributors: {
    architecture: 1,
    "request-flows": 2,
    concurrency: 3,
    storage: 4,
    execution: 5,
    persistence: 6,
    replication: 7,
    clustering: 8,
    consistency: 9,
    blocking: 10,
    vll: 11,
    protocol: 12,
    connection: 13,
    testing: 14,
    debugging: 15,
    glossary: 16,
  },
  operators: {
    configuration: 1,
    persistence: 2,
    replication: 3,
    clustering: 4,
    "backup-restore": 5,
    deployment: 6,
    lifecycle: 7,
    security: 8,
    monitoring: 9,
    eviction: 10,
    troubleshooting: 11,
  },
  users: {
    "getting-started": 1,
    commands: 2,
    compatibility: 3,
    transactions: 4,
    "lua-scripting": 5,
    "pub-sub": 6,
    "event-sourcing": 7,
    limits: 8,
  },
};

/** Convert a doc filename to a URL-friendly slug (files are already lowercase/slugified). */
function filenameToSlug(filename: string): string {
  return filename.replace(/\.md$/, "");
}

/** Extract the title from the first H1 heading, stripping "FrogDB " prefix. */
function extractTitle(content: string): string {
  const match = content.match(/^#\s+(.+)$/m);
  if (!match) return "Untitled";
  let title = match[1].trim();
  if (title.startsWith("FrogDB ")) {
    title = title.slice("FrogDB ".length);
  }
  return title;
}

/** Extract a meta description from the first paragraph after the H1. */
function extractDescription(content: string): string {
  const lines = content.split("\n");
  let pastHeading = false;
  let desc = "";
  for (const line of lines) {
    if (!pastHeading) {
      if (line.startsWith("# ")) pastHeading = true;
      continue;
    }
    const trimmed = line.trim();
    if (trimmed === "") {
      if (desc) break;
      continue;
    }
    if (
      trimmed.startsWith("#") ||
      trimmed.startsWith("```") ||
      trimmed.startsWith("|") ||
      trimmed.startsWith("- ") ||
      trimmed.startsWith("1. ")
    )
      break;
    desc += (desc ? " " : "") + trimmed;
  }
  desc = desc
    .replace(/\*\*([^*]+)\*\*/g, "$1")
    .replace(/\*([^*]+)\*/g, "$1")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");
  if (desc.length > 160) {
    desc = desc.slice(0, 157) + "...";
  }
  return desc.replace(/"/g, '\\"');
}

/** Transform markdown links to Starlight-compatible paths. */
function transformLinks(content: string, section: SyncSection): string {
  // 1. Strip ../todo/ links → plain text
  content = content.replace(
    /\[([^\]]+)\]\((?:\.\.\/)*todo\/[^)]*\)/g,
    "$1",
  );

  // 2. Cross-directory links: [text](../dirname/file.md#anchor) → [text](/prefix/slug/#anchor)
  content = content.replace(
    /\[([^\]]+)\]\(\.\.\/([^/]+)\/([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
    (_, text, dir, file, anchor) => {
      const prefix = DIR_TO_PREFIX[dir];
      if (!prefix) return `[${text}](../${dir}/${file}${anchor || ""})`;
      const slug = filenameToSlug(file);
      return `[${text}](${prefix}/${slug}/${anchor || ""})`;
    },
  );

  // 3. Same-directory links: [text](file.md#anchor) → [text](/prefix/slug/#anchor)
  content = content.replace(
    /\[([^\]]+)\]\(([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
    (_, text, file, anchor) => {
      const slug = filenameToSlug(file);
      return `[${text}](${section.urlPrefix}/${slug}/${anchor || ""})`;
    },
  );

  return content;
}

/** Build Starlight frontmatter YAML. */
function buildFrontmatter(
  title: string,
  description: string,
  order: number,
  label?: string,
): string {
  const lines = [
    "---",
    `title: "${title}"`,
    `description: "${description}"`,
    "sidebar:",
    `  order: ${order}`,
  ];
  if (label) {
    lines.push(`  label: "${label}"`);
  }
  lines.push("---", "");
  return lines.join("\n");
}

/** Process a single doc file: extract metadata, transform, and write output. */
async function processFile(
  filePath: string,
  section: SyncSection,
  sectionName: string,
): Promise<void> {
  const content = await readFile(filePath, "utf-8");
  const filename = basename(filePath);
  const stem = filenameToSlug(filename);

  const title = extractTitle(content);
  const description = extractDescription(content);

  const orderMap = ORDER_MAP[sectionName] ?? {};
  const order = orderMap[stem] ?? 50;

  // Strip the original H1 heading (Starlight renders title from frontmatter)
  let body = content.replace(/^#\s+.+\n+/, "");
  body = transformLinks(body, section);

  const frontmatter = buildFrontmatter(title, description, order);
  const output = frontmatter + body;

  await mkdir(section.outDir, { recursive: true });
  await writeFile(join(section.outDir, `${stem}.md`), output);
}

/** Sync all docs to the website content directory. */
async function syncAll(): Promise<void> {
  let total = 0;

  for (const section of SECTIONS) {
    await rm(section.outDir, { recursive: true, force: true });
    await mkdir(section.outDir, { recursive: true });

    const sectionName = basename(section.sourceDir);
    const files = (await readdir(section.sourceDir)).filter((f) =>
      f.endsWith(".md"),
    );

    await Promise.all(
      files.map((f) =>
        processFile(join(section.sourceDir, f), section, sectionName),
      ),
    );

    total += files.length;
  }

  console.log(`Synced ${total} docs across ${SECTIONS.length} sections`);
}

// --- Main ---

await syncAll();

if (process.argv.includes("--watch")) {
  console.log("Watching docs/ for changes...");

  let timeout: ReturnType<typeof setTimeout> | null = null;
  const debouncedSync = () => {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(async () => {
      try {
        await syncAll();
      } catch (err) {
        console.error("Sync error:", err);
      }
    }, 200);
  };

  for (const section of SECTIONS) {
    watch(section.sourceDir, { recursive: true }, debouncedSync);
  }
}
