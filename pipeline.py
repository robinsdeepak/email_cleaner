#!/usr/bin/env python3
"""
Gmail AI Email Cleaner - Pipeline Entrypoint.
Convenience wrapper that forwards execution to the src/ package.
"""

import os
import sys

# Ensure src/ is on python search path
_src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from gmail_cleaner.cli import main

if __name__ == "__main__":
    main()
