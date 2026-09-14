import argparse
import logging
import sys
import time

from gmail_cleaner.config import (
    GMAIL_USER,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_SNIPPET_LENGTH,
)
from gmail_cleaner.logger import get_logger, setup_logger, set_console_level
from gmail_cleaner.stages import (
    run_fetch,
    run_scan,
    run_validate,
    run_revalidate,
    run_delete,
    run_restore,
)
from gmail_cleaner.streaming import run_streaming_pipeline

logger = get_logger("cli")


def run_all_pipeline(limit=100, direction="oldest-first", workers=DEFAULT_MAX_WORKERS,
                     batch_size=DEFAULT_BATCH_SIZE, snippet_length=DEFAULT_SNIPPET_LENGTH,
                     reset_cursor=False, auto_delete=False, email_addr=None,
                     input_file=None, only_kept=False):
    """
    Executes the entire pipeline end-to-end:
    1. Fetch (single IMAP connection) - skipped if input_file is provided
    2. Scan (parallel Gemini AI)
    3. Validate (LLM Safety Auditor)
    4. Revalidate (Review Packaging)
    5. Delete (only if auto_delete is True)
    """
    target_account = email_addr or GMAIL_USER
    pipeline_t0 = time.time()
    logger.info("=" * 70)
    logger.info("🚀 [STARTING FULL EMAIL CLEANER PIPELINE]")
    logger.info(f"   • Account    : {target_account}")
    if input_file:
        logger.info(f"   • Input File : {input_file} (skipping IMAP fetch)")
    else:
        logger.info(f"   • Limit      : {limit} emails")
    logger.info(f"   • Concurrency: {workers} workers")
    logger.info(f"   • Auto-Delete: {auto_delete}")
    logger.info("=" * 70)

    # Step 1: Fetch
    if input_file:
        scan_input = input_file
        logger.info(f"[Step 1/4] Using pre-existing input dataset: {input_file}")
    else:
        logger.info("[Step 1/4] Initiating Step 1: Fetch emails via IMAP...")
        scan_input = run_fetch(
            limit=limit,
            direction=direction,
            reset_cursor=reset_cursor,
            snippet_length=snippet_length,
            email_addr=target_account
        )
        if not scan_input:
            logger.warning("⚠️ Pipeline ended: No emails fetched.")
            return

    # Step 2: Scan
    logger.info("[Step 2/4] Initiating Step 2: AI Classification...")
    scan_file = run_scan(
        input_file=scan_input,
        batch_size=batch_size,
        workers=workers,
        email_addr=target_account,
        only_kept=only_kept
    )
    if not scan_file:
        logger.warning("⚠️ Pipeline ended: Scan failed.")
        return

    # Step 3: Validate
    logger.info("[Step 3/4] Initiating Step 3: Safety Validation Audit...")
    validate_file = run_validate(
        input_file=scan_file,
        batch_size=batch_size,
        workers=workers,
        email_addr=target_account
    )
    if not validate_file:
        logger.warning("⚠️ Pipeline ended: Validation failed.")
        return

    # Step 4: Revalidate
    logger.info("[Step 4/4] Initiating Step 4: Final Review Packaging...")
    revalidate_file = run_revalidate(
        input_file=validate_file,
        email_addr=target_account
    )
    if not revalidate_file:
        logger.warning("⚠️ Pipeline ended: Revalidation failed.")
        return

    elapsed_all = time.time() - pipeline_t0
    logger.info("=" * 70)
    logger.info(f"🎉 [PIPELINE AUDIT COMPLETE] in {elapsed_all:.2f}s")
    logger.info(f"📁 Reviewed Artifact Ready: {revalidate_file}")
    logger.info("=" * 70)

    # Step 5: Delete (if requested)
    if auto_delete:
        logger.info("Proceeding with live deletion as requested (--auto-delete)...")
        run_delete(input_file=revalidate_file, dry_run=False, email_addr=target_account)
    else:
        logger.info("👉 To preview deletions: make dry-run (or: python pipeline.py dry-run)")
        logger.info("👉 To permanently move confirmed emails to Gmail Trash: make delete (or: python pipeline.py delete)")


def main():
    parser = argparse.ArgumentParser(
        prog="gmail-cleaner",
        description="Gmail AI Email Cleaner - Modular Multi-Step Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging to console")
    subparsers = parser.add_subparsers(dest="command", help="Pipeline step to run")

    # Step 1: Fetch
    p_fetch = subparsers.add_parser("fetch", help="Step 1: Fetch emails via single safe IMAP connection")
    p_fetch.add_argument("--limit", type=int, default=100)
    p_fetch.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first")
    p_fetch.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH)
    p_fetch.add_argument("--reset-cursor", action="store_true")
    p_fetch.add_argument("--output", type=str, default=None)
    p_fetch.add_argument("--email", type=str, default=None)

    # Step 2: Scan
    p_scan = subparsers.add_parser("scan", help="Step 2: Classify emails with Gemini AI")
    p_scan.add_argument("--input", type=str, default=None)
    p_scan.add_argument("--output", type=str, default=None)
    p_scan.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_scan.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    p_scan.add_argument("--only-kept", action="store_true", help="Only scan rows previously marked as KEEP")
    p_scan.add_argument("--email", type=str, default=None)

    # Step 3: Validate
    p_val = subparsers.add_parser("validate", help="Step 3: Validate deletion candidates with LLM Safety Auditor")
    p_val.add_argument("--input", type=str, default=None)
    p_val.add_argument("--output", type=str, default=None)
    p_val.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_val.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    p_val.add_argument("--email", type=str, default=None)

    # Step 4: Revalidate
    p_reval = subparsers.add_parser("revalidate", help="Step 4: Heuristic sanity audit and human review prep")
    p_reval.add_argument("--input", type=str, default=None)
    p_reval.add_argument("--output", type=str, default=None)
    p_reval.add_argument("--email", type=str, default=None)

    # Step 5: Delete
    p_del = subparsers.add_parser("delete", help="Step 5: Apply deletions to Gmail Trash")
    p_del.add_argument("--input", type=str, default=None)
    p_del.add_argument("--dry-run", action="store_true")
    p_del.add_argument("--email", type=str, default=None)

    # Step 5 Preview (dry-run shorthand)
    p_dry = subparsers.add_parser("dry-run", help="Step 5 Preview: Simulate deletion without touching Gmail")
    p_dry.add_argument("--input", type=str, default=None)
    p_dry.add_argument("--email", type=str, default=None)

    # Step 6: Restore / Undo
    p_rest = subparsers.add_parser("restore", aliases=["undo"], help="Step 6: Undo deletion and restore emails to Inbox")
    p_rest.add_argument("--input", type=str, default=None)
    p_rest.add_argument("--dry-run", action="store_true")
    p_rest.add_argument("--email", type=str, default=None)

    # Run All
    p_all = subparsers.add_parser("run-all", help="Execute entire pipeline end-to-end (staged mode)")
    p_all.add_argument("--input", type=str, default=None, help="Use existing local CSV dataset (skips Step 1 IMAP fetch)")
    p_all.add_argument("--only-kept", action="store_true", help="Only scan rows previously marked as KEEP")
    p_all.add_argument("--limit", type=int, default=100)
    p_all.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first")
    p_all.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    p_all.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_all.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH)
    p_all.add_argument("--reset-cursor", action="store_true")
    p_all.add_argument("--auto-delete", action="store_true", help="Automatically delete without pausing for review")
    p_all.add_argument("--email", type=str, default=None)

    # Stream (High-speed streaming mode)
    p_stream = subparsers.add_parser("stream", help="Fast streaming pipeline (overlapped fetch, scan, audit & live CSV flush)")
    p_stream.add_argument("--limit", type=int, default=100)
    p_stream.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first")
    p_stream.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    p_stream.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_stream.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH)
    p_stream.add_argument("--fetch-conns", type=int, default=3, help="Number of parallel IMAP connections (1-4)")
    p_stream.add_argument("--tier", choices=["paid", "free"], default="paid", help="Gemini API tier pacing")
    p_stream.add_argument("--reset-cursor", action="store_true")
    p_stream.add_argument("--auto-delete", action="store_true", help="Automatically delete without pausing for review")
    p_stream.add_argument("--email", type=str, default=None)

    args = parser.parse_args()

    target_email = getattr(args, "email", None)
    setup_logger(email_addr=target_email)
    if getattr(args, "verbose", False):
        set_console_level(logging.DEBUG)

    logger.debug(f"CLI invoked: command='{args.command}', argv={sys.argv[1:]}")

    if args.command == "fetch":
        run_fetch(limit=args.limit, direction=args.direction, output_file=args.output,
                  reset_cursor=args.reset_cursor, snippet_length=args.snippet_length, email_addr=args.email)
    elif args.command == "scan":
        run_scan(input_file=args.input, output_file=args.output, batch_size=args.batch_size,
                 workers=args.workers, email_addr=args.email, only_kept=args.only_kept)
    elif args.command == "validate":
        run_validate(input_file=args.input, output_file=args.output, batch_size=args.batch_size,
                     workers=args.workers, email_addr=args.email)
    elif args.command == "revalidate":
        run_revalidate(input_file=args.input, output_file=args.output, email_addr=args.email)
    elif args.command == "delete":
        run_delete(input_file=args.input, dry_run=args.dry_run, email_addr=args.email)
    elif args.command == "dry-run":
        run_delete(input_file=args.input, dry_run=True, email_addr=args.email)
    elif args.command in ("restore", "undo"):
        run_restore(input_file=args.input, dry_run=args.dry_run, email_addr=args.email)
    elif args.command == "run-all":
        run_all_pipeline(limit=args.limit, direction=args.direction, workers=args.workers,
                         batch_size=args.batch_size, snippet_length=args.snippet_length,
                         reset_cursor=args.reset_cursor, auto_delete=args.auto_delete, email_addr=args.email,
                         input_file=args.input, only_kept=args.only_kept)
    elif args.command == "stream":
        run_streaming_pipeline(limit=args.limit, direction=args.direction, workers=args.workers,
                               batch_size=args.batch_size, snippet_length=args.snippet_length,
                               reset_cursor=args.reset_cursor, auto_delete=args.auto_delete,
                               email_addr=args.email, fetch_conns=args.fetch_conns, tier=args.tier)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
