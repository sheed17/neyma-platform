#!/usr/bin/env python3
"""
Verify OPENAI_API_KEY and openai package for the project.
Run from project root: python3 scripts/verify_openai_env.py
Does not print the key; only reports whether it is set and if openai is usable.
"""
import os
import sys
from pathlib import Path

# Project root = parent of scripts/
project_root = Path(__file__).resolve().parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

key = os.getenv("OPENAI_API_KEY")
key_set = bool(key and str(key).strip())
key_preview = "(not set)" if not key_set else f"set ({len(str(key))} chars)"

print("OPENAI_API_KEY:", key_preview)

try:
    import openai
    print("openai package: installed")
    if key_set:
        client = openai.OpenAI()
        # Minimal check: create client (no API call unless you want to ping)
        print("OpenAI client: OK (key will be used when moderation/LLM is called)")
except ImportError:
    print("openai package: NOT installed — run: pip install openai")
    sys.exit(1)
except Exception as e:
    print("OpenAI client error:", e)
    sys.exit(1)

if not key_set:
    print("\nTo set the key: add OPENAI_API_KEY=sk-... to .env in project root, then restart the backend.")
    print("If you ever exposed the key in chat or in a repo, rotate it at platform.openai.com.")
    sys.exit(1)

print("Environment check passed.")
