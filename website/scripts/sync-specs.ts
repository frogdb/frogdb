#!/usr/bin/env bun
/**
 * Syncs docs/spec/ markdown files into website/src/content/docs/architecture/
 * with Starlight-compatible frontmatter and transformed internal links.
 *
 * Usage:
 *   bun run scripts/sync-specs.ts          # one-shot sync
 *   bun run scripts/sync-specs.ts --watch  # sync + watch for changes
 */

import { readdir, readFile, writeFile, mkdir, rm } from "node:fs/promises";
import { join, basename, relative } from "node:path";
import { watch } from "node:fs";

const SPEC_DIR = join(import.meta.dir, "../../docs/spec");
const OUT_DIR = join(import.meta.dir, "../src/content/docs/architecture");

// Sidebar ordering by category
const ORDER_MAP: Record<string, number> = {
  // Core
  INDEX: 1,
  ARCHITECTURE: 2,
  REPO: 3,
  CONCURRENCY: 4,
  STORAGE: 5,
  EXECUTION: 6,
  REQUEST_FLOWS: 7,
  // Features
  PERSISTENCE: 11,
  REPLICATION: 12,
  CLUSTER: 13,
  CONSISTENCY: 14,
  TRANSACTIONS: 15,
  BLOCKING: 16,
  PUBSUB: 17,
  SCRIPTING: 18,
  VLL: 19,
  EVICTION: 20,
  TIERED: 21,
  EVENT_SOURCING: 22,
  // Infrastructure
  AUTH: 31,
  TLS: 32,
  CONNECTION: 33,
  CONFIGURATION: 34,
  PROTOCOL: 35,
  LIFECYCLE: 36,
  DEPLOYMENT: 37,
  // Observability & Operations
  OBSERVABILITY: 46,
  DEBUGGING: 47,
  TROUBLESHOOTING: 48,
  "PROFILING-RESULTS": 49,
  // Reference
  COMMANDS: 56,
  COMPATIBILITY: 57,
  LIMITS: 58,
  FAILURE_MODES: 59,
  TESTING: 60,
  ROADMAP: 61,
  GLOSSARY: 62,
  SOURCES: 63,
};

/** Convert a spec filename to a URL-friendly slug. */
function filenameToSlug(filename: string): string {
  const name = filename.replace(/\.md$/, "");
  if (name === "INDEX") return "overview";
  return name.toLowerCase().replace(/_/g, "-");
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
    // Stop at non-paragraph content
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
  // Strip markdown formatting for a clean meta description
  desc = desc
    .replace(/\*\*([^*]+)\*\*/g, "$1") // bold
    .replace(/\*([^*]+)\*/g, "$1") // italic
    .replace(/`([^`]+)`/g, "$1") // inline code
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1"); // links
  if (desc.length > 160) {
    desc = desc.slice(0, 157) + "...";
  }
  return desc.replace(/"/g, '\\"');
}

/** Transform markdown links to Starlight-compatible paths. */
function transformLinks(content: string, isTypesFile: boolean): string {
  // 1. Strip ../todo/ and ../../todo/ links → plain text (must run first)
  content = content.replace(
    /\[([^\]]+)\]\((?:\.\.\/)+todo\/[^)]*\)/g,
    "$1",
  );

  // 2. Directory link: [text](types/) → [text](/architecture/types/)
  content = content.replace(
    /\[([^\]]+)\]\(types\/\)/g,
    "[$1](/architecture/types/)",
  );

  if (isTypesFile) {
    // 3. From types/ files: [text](../FILE.md#anchor) → [text](/architecture/slug/#anchor)
    content = content.replace(
      /\[([^\]]+)\]\(\.\.\/([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
      (_, text, file, anchor) => {
        const slug = filenameToSlug(file);
        return `[${text}](/architecture/${slug}/${anchor || ""})`;
      },
    );
    // 4. Types files linking to sibling type files: [text](FILE.md#anchor)
    content = content.replace(
      /\[([^\]]+)\]\(([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
      (_, text, file, anchor) => {
        const slug = filenameToSlug(file);
        return `[${text}](/architecture/types/${slug}/${anchor || ""})`;
      },
    );
  } else {
    // 5. Root files: [text](types/FILE.md#anchor) → [text](/architecture/types/slug/#anchor)
    content = content.replace(
      /\[([^\]]+)\]\(types\/([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
      (_, text, file, anchor) => {
        const slug = filenameToSlug(file);
        return `[${text}](/architecture/types/${slug}/${anchor || ""})`;
      },
    );
    // 6. Root files: [text](FILE.md#anchor) → [text](/architecture/slug/#anchor)
    content = content.replace(
      /\[([^\]]+)\]\(([A-Za-z][A-Za-z0-9_-]*\.md)(#[^)]+)?\)/g,
      (_, text, file, anchor) => {
        const slug = filenameToSlug(file);
        return `[${text}](/architecture/${slug}/${anchor || ""})`;
      },
    );
  }

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

/** Process a single spec file: extract metadata, transform, and write output. */
async function processFile(
  filePath: string,
  isTypesFile: boolean,
): Promise<void> {
  const content = await readFile(filePath, "utf-8");
  const filename = basename(filePath);
  const stem = filename.replace(/\.md$/, "");
  const slug = filenameToSlug(filename);

  const title = extractTitle(content);
  const description = extractDescription(content);

  const order = isTypesFile ? 100 : (ORDER_MAP[stem] ?? 50);
  const label = stem === "INDEX" ? "Overview" : undefined;

  // Strip the original H1 heading (Starlight renders title from frontmatter)
  let body = content.replace(/^#\s+.+\n+/, "");
  body = transformLinks(body, isTypesFile);

  const frontmatter = buildFrontmatter(title, description, order, label);
  const output = frontmatter + body;

  const outSubDir = isTypesFile ? join(OUT_DIR, "types") : OUT_DIR;
  await mkdir(outSubDir, { recursive: true });
  await writeFile(join(outSubDir, `${slug}.md`), output);
}

/** Sync all spec docs to the website content directory. */
async function syncAll(): Promise<void> {
  await rm(OUT_DIR, { recursive: true, force: true });
  await mkdir(OUT_DIR, { recursive: true });
  await mkdir(join(OUT_DIR, "types"), { recursive: true });

  // Collect all files
  const rootFiles = (await readdir(SPEC_DIR))
    .filter((f) => f.endsWith(".md"));
  const typesFiles = (await readdir(join(SPEC_DIR, "types")))
    .filter((f) => f.endsWith(".md"));

  // Process all in parallel
  await Promise.all([
    ...rootFiles.map((f) => processFile(join(SPEC_DIR, f), false)),
    ...typesFiles.map((f) => processFile(join(SPEC_DIR, "types", f), true)),
  ]);

  const total = rootFiles.length + typesFiles.length;
  console.log(
    `Synced ${total} spec docs to ${relative(process.cwd(), OUT_DIR)}`,
  );
}

// --- Main ---

await syncAll();

if (process.argv.includes("--watch")) {
  console.log("Watching docs/spec/ for changes...");

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

  watch(SPEC_DIR, { recursive: true }, debouncedSync);
}
