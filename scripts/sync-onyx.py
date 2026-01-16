#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = ["requests", "beautifulsoup4", "lxml", "pypdf"]
# ///
"""
Sync FrogDB spec/ and src/ files to Onyx knowledge base via Ingestion API.

Usage:
    uv run scripts/sync-onyx.py                 # Sync spec/ and src/
    uv run scripts/sync-onyx.py --spec-only     # Sync spec/ only
    uv run scripts/sync-onyx.py --src-only      # Sync src/ only
    uv run scripts/sync-onyx.py --sources       # Sync spec/, src/, AND external URLs
    uv run scripts/sync-onyx.py --sources-only  # Sync external URLs only
    uv run scripts/sync-onyx.py --dry-run       # Show what would be synced

Environment variables:
    ONYX_API_TOKEN   - Required. Your Onyx API token.
    ONYX_API_URL     - Optional. Default: http://localhost:8080
    ONYX_CC_PAIR_ID  - Optional. Default: 1
    GITHUB_TOKEN     - Optional. For higher GitHub API rate limits.
"""

import argparse
import io
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader


def get_config() -> dict:
    """Load configuration from environment variables."""
    token = os.environ.get("ONYX_API_TOKEN")
    if not token:
        print("Error: ONYX_API_TOKEN environment variable is required", file=sys.stderr)
        print("Set it with: export ONYX_API_TOKEN='your-token-here'", file=sys.stderr)
        sys.exit(1)

    return {
        "api_url": os.environ.get("ONYX_API_URL", "http://localhost:8080"),
        "api_token": token,
        "cc_pair_id": int(os.environ.get("ONYX_CC_PAIR_ID", "1")),
        "github_token": os.environ.get("GITHUB_TOKEN"),
    }


# Rate limiting state
_last_request_time: dict[str, float] = {}
RATE_LIMIT_DELAY = 1.0  # seconds between requests to same domain


def rate_limit(url: str) -> None:
    """Apply rate limiting based on domain."""
    domain = urlparse(url).netloc
    now = time.time()
    if domain in _last_request_time:
        elapsed = now - _last_request_time[domain]
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
    _last_request_time[domain] = time.time()


def parse_sources_md(filepath: Path) -> list[dict]:
    """Parse SOURCES.md and extract URLs with metadata.

    Returns list of dicts with: url, topic, category, notes
    """
    content = filepath.read_text(encoding="utf-8")
    sources = []
    current_category = "Unknown"

    lines = content.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Track category from H2 headers
        if line.startswith("## "):
            current_category = line[3:].strip()
            i += 1
            continue

        # Parse table rows (skip header and separator)
        if line.startswith("|") and "---" not in line:
            parts = [p.strip() for p in line.split("|")]
            # Filter empty parts from leading/trailing |
            parts = [p for p in parts if p]

            if len(parts) >= 2:
                topic = parts[0]
                # Skip header rows
                if topic.lower() in ("topic", "tool", "provider", "feature"):
                    i += 1
                    continue

                # Extract URL from markdown link or plain URL
                url_cell = parts[1]
                url_match = re.search(r"https?://[^\s\)]+", url_cell)
                if url_match:
                    url = url_match.group(0)
                    notes = parts[2] if len(parts) > 2 else ""
                    sources.append({
                        "url": url,
                        "topic": topic,
                        "category": current_category,
                        "notes": notes,
                    })
        i += 1

    return sources


def categorize_url(url: str) -> str:
    """Categorize URL as 'github', 'pdf', or 'web'."""
    if url.lower().endswith(".pdf"):
        return "pdf"
    if "github.com" in url:
        return "github"
    return "web"


def fetch_web_content(url: str) -> str | None:
    """Fetch HTML page and extract main text content."""
    rate_limit(url)
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; FrogDB-Sync/1.0)"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "lxml")

        # Remove script, style, nav, footer elements
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()

        # Try to find main content area
        main = soup.find("main") or soup.find("article") or soup.find(class_="content")
        if main:
            text = main.get_text(separator="\n", strip=True)
        else:
            # Fall back to body
            body = soup.find("body")
            text = body.get_text(separator="\n", strip=True) if body else ""

        # Clean up excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text if text else None

    except requests.exceptions.RequestException as e:
        print(f"    Error fetching {url}: {e}", file=sys.stderr)
        return None


def fetch_github_content(url: str, config: dict) -> str | None:
    """Fetch content from GitHub using API for cleaner output."""
    rate_limit(url)

    parsed = urlparse(url)
    path_parts = parsed.path.strip("/").split("/")

    if len(path_parts) < 2:
        return fetch_web_content(url)  # Fall back to web fetch

    owner, repo = path_parts[0], path_parts[1]

    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "FrogDB-Sync/1.0",
    }
    if config.get("github_token"):
        headers["Authorization"] = f"token {config['github_token']}"

    try:
        # Handle issues
        if len(path_parts) >= 4 and path_parts[2] == "issues":
            issue_num = path_parts[3]
            api_url = f"https://api.github.com/repos/{owner}/{repo}/issues/{issue_num}"
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            content = f"# {data['title']}\n\n"
            content += f"**State:** {data['state']}\n"
            content += f"**Author:** {data['user']['login']}\n\n"
            content += data.get("body", "") or ""
            return content

        # Handle pull requests
        if len(path_parts) >= 4 and path_parts[2] == "pull":
            pr_num = path_parts[3]
            api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_num}"
            response = requests.get(api_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            content = f"# {data['title']}\n\n"
            content += f"**State:** {data['state']}\n"
            content += f"**Author:** {data['user']['login']}\n\n"
            content += data.get("body", "") or ""
            return content

        # Handle blob (file) URLs
        if len(path_parts) >= 4 and path_parts[2] == "blob":
            # Convert blob URL to raw URL
            branch = path_parts[3]
            file_path = "/".join(path_parts[4:])
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{file_path}"
            response = requests.get(raw_url, headers={"User-Agent": "FrogDB-Sync/1.0"}, timeout=30)
            response.raise_for_status()
            return response.text

        # Handle wiki pages
        if len(path_parts) >= 3 and path_parts[2] == "wiki":
            return fetch_web_content(url)

        # Default: fetch web content
        return fetch_web_content(url)

    except requests.exceptions.RequestException as e:
        print(f"    Error fetching GitHub {url}: {e}", file=sys.stderr)
        return fetch_web_content(url)  # Fall back to web fetch


def fetch_pdf_content(url: str) -> tuple[str | None, str | None]:
    """Download PDF and extract text content.

    Returns: (content, error_message) - content is None if failed
    """
    rate_limit(url)

    # Download PDF
    print(f"      Downloading...", end=" ", flush=True)
    download_start = time.time()
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; FrogDB-Sync/1.0)"
        }
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        download_time = time.time() - download_start
        print(f"done ({download_time:.1f}s)")
    except requests.exceptions.Timeout:
        print("TIMEOUT")
        return None, "Download timed out after 60s"
    except requests.exceptions.RequestException as e:
        print("FAILED")
        return None, f"Download failed: {e}"

    # Parse PDF
    print(f"      Extracting text...", end=" ", flush=True)
    try:
        pdf_file = io.BytesIO(response.content)
        reader = PdfReader(pdf_file)

        # Check if encrypted
        if reader.is_encrypted:
            print("ENCRYPTED")
            return None, "PDF is encrypted/password-protected"

        page_count = len(reader.pages)
        text_parts = []
        failed_pages = []

        for i, page in enumerate(reader.pages):
            try:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            except Exception as e:
                failed_pages.append(i + 1)

        if failed_pages:
            print(f"{page_count} pages, {len(failed_pages)} failed")

        if not text_parts:
            print(f"{page_count} pages, no text")
            return None, f"PDF has {page_count} pages but no extractable text (likely scanned/image PDF)"

        total_chars = sum(len(t) for t in text_parts)
        print(f"{page_count} pages, {total_chars:,} chars")

        return "\n\n".join(text_parts), None

    except Exception as e:
        print("FAILED")
        return None, f"PDF parsing failed: {e}"


def create_external_document(source: dict, content: str) -> dict:
    """Convert external source to Onyx document format."""
    url = source["url"]
    topic = source["topic"]
    category = source["category"]
    notes = source["notes"]

    # Create unique ID from URL
    parsed = urlparse(url)
    doc_id = f"external:{parsed.netloc}{parsed.path}"

    # Create semantic identifier
    semantic_id = f"{category}: {topic}"

    # Create title
    title = f"{topic} - {category}"

    return {
        "document": {
            "id": doc_id,
            "semantic_identifier": semantic_id,
            "title": title,
            "sections": [
                {
                    "text": content,
                    "link": url,
                }
            ],
            "source": "web",
            "metadata": {
                "type": "external",
                "category": category,
                "notes": notes,
                "url": url,
            },
        }
    }


def sync_external_url(
    source: dict, config: dict, dry_run: bool, index: int, total: int
) -> tuple[bool, str | None]:
    """Sync a single external URL to Onyx.

    Returns: (success, error_message)
    """
    url = source["url"]
    url_type = categorize_url(url).upper()

    print(f"  [{index}/{total}] {url_type}: {source['topic']}")
    print(f"      {url}")

    if dry_run:
        print(f"      → SKIP (dry-run)")
        return True, None

    # Fetch content based on type
    error_msg = None
    if url_type == "PDF":
        content, error_msg = fetch_pdf_content(url)
    elif url_type == "GITHUB":
        print(f"      Fetching...", end=" ", flush=True)
        fetch_start = time.time()
        content = fetch_github_content(url, config)
        fetch_time = time.time() - fetch_start
        if content:
            print(f"done ({fetch_time:.1f}s, {len(content):,} chars)")
        else:
            print("FAILED")
            error_msg = "Failed to fetch GitHub content"
    else:
        print(f"      Fetching...", end=" ", flush=True)
        fetch_start = time.time()
        content = fetch_web_content(url)
        fetch_time = time.time() - fetch_start
        if content:
            print(f"done ({fetch_time:.1f}s, {len(content):,} chars)")
        else:
            print("FAILED")
            error_msg = "Failed to fetch web content"

    if not content:
        print(f"      → FAILED: {error_msg}")
        return False, error_msg

    document = create_external_document(source, content)
    document["cc_pair_id"] = config["cc_pair_id"]

    # Send to Onyx API
    api_url = f"{config['api_url']}/onyx-api/ingestion"
    headers = {
        "Authorization": f"Bearer {config['api_token']}",
        "Content-Type": "application/json",
    }

    print(f"      Uploading...", end=" ", flush=True)
    try:
        response = requests.post(api_url, json=document, headers=headers, timeout=30)
        response.raise_for_status()
        print("OK")
        return True, None
    except requests.exceptions.RequestException as e:
        print("FAILED")
        error_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            error_msg = f"{e} - {e.response.text}"
        print(f"      → FAILED: {error_msg}")
        return False, error_msg


def extract_title(content: str, filepath: Path) -> str:
    """Extract title from markdown H1 header or use filename."""
    # Try to find first H1 header
    match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
    if match:
        return match.group(1).strip()

    # Fall back to filename without extension
    return filepath.stem.replace("-", " ").replace("_", " ").title()


def create_document(filepath: Path, content: str, doc_type: str) -> dict:
    """Convert a file to Onyx document format."""
    relative_path = str(filepath)
    title = extract_title(content, filepath)

    # Create semantic identifier (shorter, for display)
    if doc_type == "spec":
        semantic_id = f"FrogDB Spec: {title}"
    else:
        semantic_id = f"FrogDB Source: {filepath.name}"

    return {
        "document": {
            "id": relative_path,
            "semantic_identifier": semantic_id,
            "title": f"{title} - FrogDB",
            "sections": [
                {
                    "text": content,
                    "link": relative_path,
                }
            ],
            "source": "file",
            "metadata": {
                "type": doc_type,
                "path": relative_path,
            },
        }
    }


def sync_file(
    filepath: Path, doc_type: str, config: dict, dry_run: bool, index: int, total: int
) -> tuple[bool, str | None]:
    """Sync a single file to Onyx.

    Returns: (success, error_message)
    """
    print(f"  [{index}/{total}] {filepath}", end=" ", flush=True)

    try:
        content = filepath.read_text(encoding="utf-8")
    except Exception as e:
        print(f"→ FAILED")
        error_msg = f"Error reading file: {e}"
        print(f"      {error_msg}")
        return False, error_msg

    document = create_document(filepath, content, doc_type)
    document["cc_pair_id"] = config["cc_pair_id"]

    if dry_run:
        print("→ SKIP (dry-run)")
        return True, None

    # Send to Onyx API
    url = f"{config['api_url']}/onyx-api/ingestion"
    headers = {
        "Authorization": f"Bearer {config['api_token']}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(url, json=document, headers=headers, timeout=30)
        response.raise_for_status()
        print("→ OK")
        return True, None
    except requests.exceptions.RequestException as e:
        print("→ FAILED")
        error_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            error_msg = f"{e} - {e.response.text}"
        print(f"      {error_msg}")
        return False, error_msg


def find_files(base_path: Path, patterns: list[str]) -> list[Path]:
    """Find all files matching the given glob patterns."""
    files = []
    for pattern in patterns:
        files.extend(base_path.glob(pattern))
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(
        description="Sync FrogDB files to Onyx knowledge base"
    )
    parser.add_argument(
        "--spec-only", action="store_true", help="Only sync spec/ directory"
    )
    parser.add_argument(
        "--src-only", action="store_true", help="Only sync src/ directory"
    )
    parser.add_argument(
        "--sources", action="store_true", help="Also sync external URLs from spec/SOURCES.md"
    )
    parser.add_argument(
        "--sources-only", action="store_true", help="Only sync external URLs from spec/SOURCES.md"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without sending",
    )
    args = parser.parse_args()

    # Handle conflicting flags
    if args.sources_only and (args.spec_only or args.src_only):
        print("Error: --sources-only cannot be combined with --spec-only or --src-only", file=sys.stderr)
        sys.exit(1)

    # Find repo root (where this script is in scripts/)
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent

    config = get_config()

    # Configuration header
    print("=" * 50)
    print("=== Configuration ===")
    print(f"API URL: {config['api_url']}")
    print(f"CC Pair ID: {config['cc_pair_id']}")
    if args.dry_run:
        print("Mode: DRY RUN (no files will be synced)")
    print()

    total_files = 0
    success_count = 0
    error_count = 0
    errors: list[str] = []

    # Sync spec/ directory
    if not args.src_only and not args.sources_only:
        spec_dir = repo_root / "spec"
        if spec_dir.exists():
            spec_files = find_files(spec_dir, ["**/*.md"])
            total_files += len(spec_files)
            print(f"=== Syncing spec/ ({len(spec_files)} files) ===")

            for i, filepath in enumerate(spec_files, 1):
                relative = filepath.relative_to(repo_root)
                success, error = sync_file(relative, "spec", config, args.dry_run, i, len(spec_files))
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"{relative}: {error}")
            print()
        else:
            print(f"Warning: spec/ directory not found at {spec_dir}")
            print()

    # Sync src/ directory
    if not args.spec_only and not args.sources_only:
        src_dir = repo_root / "src"
        if src_dir.exists():
            src_files = find_files(src_dir, ["**/*.rs"])
            total_files += len(src_files)
            print(f"=== Syncing src/ ({len(src_files)} files) ===")

            for i, filepath in enumerate(src_files, 1):
                relative = filepath.relative_to(repo_root)
                success, error = sync_file(relative, "source", config, args.dry_run, i, len(src_files))
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"{relative}: {error}")
            print()
        else:
            print(f"Note: src/ directory not found at {src_dir} (skipping)")
            print()

    # Sync external sources from SOURCES.md
    if args.sources or args.sources_only:
        sources_file = repo_root / "spec" / "SOURCES.md"
        if sources_file.exists():
            sources = parse_sources_md(sources_file)
            total_files += len(sources)

            # Group by category for display
            categories: dict[str, int] = {}
            for s in sources:
                cat = s["category"]
                categories[cat] = categories.get(cat, 0) + 1

            print(f"=== Syncing external sources ({len(sources)} URLs) ===")
            for cat, count in sorted(categories.items()):
                print(f"  {cat}: {count}")
            print()

            for i, source in enumerate(sources, 1):
                success, error = sync_external_url(source, config, args.dry_run, i, len(sources))
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    errors.append(f"{source['url']}: {error}")
            print()
        else:
            print(f"Warning: spec/SOURCES.md not found at {sources_file}")
            print()

    # Summary
    print("=" * 50)
    print("=== Summary ===")
    if args.dry_run:
        print(f"Would sync: {total_files} files")
    else:
        print(f"Synced: {success_count}/{total_files}")
        if error_count > 0:
            print(f"Errors: {error_count}")
            print()
            print("Failed items:")
            for err in errors:
                print(f"  - {err}")
            sys.exit(1)

    print()
    print("Done!")


if __name__ == "__main__":
    main()
