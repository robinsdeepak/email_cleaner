# ==============================================================================
# Gmail AI Email Cleaner - Modular Pipeline Makefile
# ==============================================================================

PYTHON ?= python3
LIMIT ?= 100
WORKERS ?= 10
BATCH_SIZE ?= 50
EMAIL ?=
INPUT ?=

.PHONY: help fetch scan validate revalidate dry-run delete run-all undo test-limits stress-test clean

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
	@echo "  make stream [LIMIT=100]"
	@echo "      High-speed streaming pipeline (overlapped I/O, Gemini & live disk flush)"
	@echo ""
	@echo "  make undo"
	@echo "      Step 6: Undo deletion and restore emails back to Inbox"
	@echo ""
	@echo "  make test-limits"
	@echo "      Test active Gemini rate limit (Free vs Paid Tier)"
	@echo ""
	@echo "  make clean"
	@echo "      Clean python bytecode and test files"
	@echo "=================================================================="

fetch:
	$(PYTHON) pipeline.py fetch --limit $(LIMIT) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

scan:
	$(PYTHON) pipeline.py scan --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

validate:
	$(PYTHON) pipeline.py validate --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

revalidate:
	$(PYTHON) pipeline.py revalidate $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

dry-run:
	$(PYTHON) pipeline.py delete --dry-run $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

delete:
	$(PYTHON) pipeline.py delete $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

run-all:
	$(PYTHON) pipeline.py run-all --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

stream:
	$(PYTHON) pipeline.py stream --limit $(LIMIT) --workers $(WORKERS) --batch-size $(BATCH_SIZE) $(if $(EMAIL),--email $(EMAIL),) $(EXTRA_ARGS)

undo:
	$(PYTHON) pipeline.py restore $(if $(INPUT),--input $(INPUT),) $(if $(EMAIL),--email $(EMAIL),)

test-limits:
	$(PYTHON) tools/check_rate_limit.py

stress-test:
	$(PYTHON) tools/stress_test_limits.py --test all

clean:
	rm -rf __pycache__ *.pyc outputs/*/*/*test*.csv
	@echo "🧹 Cleaned temporary files."
