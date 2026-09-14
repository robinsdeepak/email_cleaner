# ==============================================================================
# Gmail AI Email Cleaner - Modular Pipeline Makefile
# ==============================================================================

PYTHON ?= $(shell if [ -f /Users/mac/.python_envs/global/bin/python ]; then echo /Users/mac/.python_envs/global/bin/python; elif [ -f venv/bin/python ]; then echo venv/bin/python; elif [ -f .venv/bin/python ]; then echo .venv/bin/python; else which python3; fi)
LIMIT ?= 100
WORKERS ?= 10
BATCH_SIZE ?= 50
EMAIL ?=
INPUT ?=

.PHONY: help fetch scan validate revalidate dry-run delete run-all run-all-awake stream stream-awake undo test-limits stress-test clean ui db-import db-export db-refill runs

.DEFAULT_GOAL := help

help:
	@echo "=================================================================="
	@echo " 📧 Gmail AI Email Cleaner - Modular Multi-Step Pipeline"
	@echo "=================================================================="
	@echo "  make fetch [LIMIT=100]"
	@echo "      Step 1: Fetch emails via single safe IMAP connection -> 1_fetch/"
	@echo ""
	@echo "  make scan [WORKERS=10] [BATCH_SIZE=50]"
	@echo "      Step 2: Classify emails with Gemini AI (0 IMAP calls) -> 2_scan/"
	@echo ""
	@echo "  make validate [WORKERS=10]"
	@echo "      Step 3: Run LLM False-Positive Auditor -> 3_validate/"
	@echo ""
	@echo "  make revalidate"
	@echo "      Step 4: Run Heuristic Sanity Auditor -> 4_revalidate/"
	@echo ""
	@echo "  make dry-run"
	@echo "      Step 5 Preview: Simulate deletion without touching Gmail"
	@echo ""
	@echo "  make delete"
	@echo "      Step 5 Live: Move confirmed emails to Gmail Trash -> 5_processed/"
	@echo ""
	@echo "  make run-all [LIMIT=100]"
	@echo "      Execute Steps 1-4 end-to-end (stops before delete for review)"
	@echo ""
	@echo "  make run-all-awake [LIMIT=100]"
	@echo "      Run-all with sleep prevention (uses 'caffeinate -i' to keep Mac awake when locked)"
	@echo ""
	@echo "  make stream [LIMIT=100]"
	@echo "      High-speed streaming pipeline (overlapped I/O, Gemini & live disk flush)"
	@echo ""
	@echo "  make stream-awake [LIMIT=100]"
	@echo "      Stream with sleep prevention (uses 'caffeinate -i' to keep Mac awake when locked)"
	@echo ""
	@echo "  make undo"
	@echo "      Step 6: Undo deletion and restore emails back to Inbox"
	@echo ""
	@echo "  make test-limits"
	@echo "      Test active Gemini rate limit (Free vs Paid Tier)"
	@echo ""
	@echo "  make stress-test"
	@echo "      Stress test Gemini rate limits"
	@echo ""
	@echo "  make ui"
	@echo "      Launch the interactive Streamlit Web Dashboard"
	@echo ""
	@echo "  make db-import [INPUT=path/to/file.csv]"
	@echo "      Import CSV review artifact directly into SQLite database"
	@echo ""
	@echo "  make db-export [OUTPUT=path/to/file.csv]"
	@echo "      Export SQLite database to CSV artifact"
	@echo ""
	@echo "  make db-refill [LIMIT=500] [BATCH_SIZE=100]"
	@echo "      Backfill missing email snippets from Gmail using 10KB peek buffer"
	@echo ""
	@echo "  make runs [LIMIT=20]"
	@echo "      View past pipeline execution history, run IDs, and duration stats"
	@echo ""
	@echo "  make clean"
	@echo "      Clean python bytecode and test files"
	@echo "=================================================================="

fetch:
	$(PYTHON) pipeline.py fetch --limit $(LIMIT) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

scan:
	$(PYTHON) pipeline.py scan --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(INPUT),--input $(INPUT),) $(if $(ONLY_KEPT),--only-kept,) $(if $(EMAIL),--email $(EMAIL),)

validate:
	$(PYTHON) pipeline.py validate --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

revalidate:
	$(PYTHON) pipeline.py revalidate $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

dry-run:
	$(PYTHON) pipeline.py delete --dry-run $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

delete:
	$(PYTHON) pipeline.py delete $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

run-all:
	$(PYTHON) pipeline.py run-all $(if $(INPUT),--input $(INPUT),) $(if $(ONLY_KEPT),--only-kept,) --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

run-all-awake:
	@if command -v caffeinate >/dev/null 2>&1; then \
		echo "☕ Running with caffeinate (Mac sleep disabled)..."; \
		caffeinate -i $(PYTHON) pipeline.py run-all $(if $(INPUT),--input $(INPUT),) $(if $(ONLY_KEPT),--only-kept,) --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS); \
	else \
		echo "⚠️ 'caffeinate' is only available on macOS. Running standard pipeline..."; \
		$(PYTHON) pipeline.py run-all $(if $(INPUT),--input $(INPUT),) $(if $(ONLY_KEPT),--only-kept,) --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS); \
	fi

stream:
	$(PYTHON) pipeline.py stream --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

stream-awake:
	@if command -v caffeinate >/dev/null 2>&1; then \
		echo "☕ Running with caffeinate (Mac sleep disabled)..."; \
		caffeinate -i $(PYTHON) pipeline.py stream --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS); \
	else \
		echo "⚠️ 'caffeinate' is only available on macOS. Running standard stream..."; \
		$(PYTHON) pipeline.py stream --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS); \
	fi

undo:
	$(PYTHON) pipeline.py restore $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

test-limits:
	$(PYTHON) tools/check_rate_limit.py

stress-test:
	$(PYTHON) tools/stress_test_limits.py --test all

ui:
	$(PYTHON) -m streamlit run app.py

db-import:
	$(PYTHON) pipeline.py db-import $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

db-export:
	$(PYTHON) pipeline.py db-export $(if $(OUTPUT),--output $(OUTPUT),) $(if $(EMAIL),--email $(EMAIL),)

db-refill:
	$(PYTHON) pipeline.py db-refill-snippets $(if $(LIMIT),--limit $(LIMIT),) $(if $(BATCH_SIZE),--batch-size $(BATCH_SIZE),) $(if $(EMAIL),--email $(EMAIL),)

runs:
	$(PYTHON) pipeline.py runs $(if $(LIMIT),--limit $(LIMIT),) $(if $(EMAIL),--email $(EMAIL),)

architecture:
	@if [ -f docs/architecture/system-map.html ]; then \
		echo "🗺️ Opening interactive Archify architecture map..."; \
		open docs/architecture/system-map.html 2>/dev/null || xdg-open docs/architecture/system-map.html 2>/dev/null || echo "Map available at docs/architecture/system-map.html"; \
	fi

clean:
	rm -rf __pycache__ src/**/__pycache__ *.pyc
	@echo "🧹 Cleaned temporary files."

