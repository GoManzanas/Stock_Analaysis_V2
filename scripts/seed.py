#!/usr/bin/env python3
"""One-command bootstrap: run the full pipeline from scratch.

Usage: python scripts/seed.py [--from-year 2014] [--to-year 2025]
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cli.main import cli

if __name__ == "__main__":
    sys.argv = ["seed", "pipeline"] + sys.argv[1:]
    cli()
