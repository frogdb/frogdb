"""CLI entrypoint for workflow generation."""

import argparse
import sys
from pathlib import Path

from workflow_gen.render import WORKFLOWS, render


def generate(output_dir: Path) -> None:
    """Generate all workflow files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, builder in WORKFLOWS.items():
        path = output_dir / filename
        content = render(builder())
        path.write_text(content)
        print(f"Generated {path}")


def check(output_dir: Path) -> bool:
    """Check that existing workflow files match generated content."""
    ok = True
    for filename, builder in WORKFLOWS.items():
        path = output_dir / filename
        expected = render(builder())
        if not path.exists():
            print(f"MISSING: {path}")
            ok = False
            continue
        actual = path.read_text()
        if actual != expected:
            print(f"OUT OF DATE: {path}")
            ok = False
        else:
            print(f"OK: {path}")
    return ok


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate GitHub Actions workflow files")
    parser.add_argument("--output", default=".github/workflows", help="Output directory")
    parser.add_argument("--check", action="store_true", help="Check files are up to date")
    args = parser.parse_args()

    output_dir = Path(args.output)

    if args.check:
        if not check(output_dir):
            print("\nWorkflow files are out of date. Run 'just workflow-gen' to regenerate.")
            sys.exit(1)
    else:
        generate(output_dir)


if __name__ == "__main__":
    main()
