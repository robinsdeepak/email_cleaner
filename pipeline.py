import argparse
import sys
from common import (
    GMAIL_USER,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_SNIPPET_LENGTH,
)
from step1_fetch import run_fetch
from step2_scan import run_scan
from step3_validate import run_validate
from step4_revalidate import run_revalidate
from step5_delete import run_delete
from step6_restore import run_restore


def run_all_pipeline(limit=100, direction="oldest-first", workers=DEFAULT_MAX_WORKERS,
                     batch_size=DEFAULT_BATCH_SIZE, snippet_length=DEFAULT_SNIPPET_LENGTH,
                     reset_cursor=False, auto_delete=False, email_addr=None):
    """
    Executes the entire pipeline end-to-end:
    1. Fetch (single IMAP connection)
    2. Scan (parallel Gemini AI)
    3. Validate (LLM Safety Auditor)
    4. Revalidate (Heuristic Sanity Auditor)
    5. Delete (only if auto_delete is True)
    """
    target_account = email_addr or GMAIL_USER
    print("\n" + "=" * 70)
    print(f"🚀 [STARTING FULL EMAIL CLEANER PIPELINE]")
    print(f"   • Account    : {target_account}")
    print(f"   • Limit      : {limit} emails")
    print(f"   • Concurrency: {workers} workers")
    print(f"   • Auto-Delete: {auto_delete}")
    print("=" * 70)

    # Step 1: Fetch
    fetch_file = run_fetch(
        limit=limit,
        direction=direction,
        reset_cursor=reset_cursor,
        snippet_length=snippet_length,
        email_addr=target_account
    )
    if not fetch_file:
        print("⚠️ Pipeline ended: No emails fetched.")
        return

    # Step 2: Scan
    scan_file = run_scan(
        input_file=fetch_file,
        batch_size=batch_size,
        workers=workers,
        email_addr=target_account
    )
    if not scan_file:
        print("⚠️ Pipeline ended: Scan failed.")
        return

    # Step 3: Validate
    validate_file = run_validate(
        input_file=scan_file,
        batch_size=batch_size,
        workers=workers,
        email_addr=target_account
    )
    if not validate_file:
        print("⚠️ Pipeline ended: Validation failed.")
        return

    # Step 4: Revalidate
    revalidate_file = run_revalidate(
        input_file=validate_file,
        email_addr=target_account
    )
    if not revalidate_file:
        print("⚠️ Pipeline ended: Revalidation failed.")
        return

    print("\n" + "=" * 70)
    print("🎉 [PIPELINE AUDIT COMPLETE]")
    print(f"📁 Reviewed Artifact Ready: {revalidate_file}")
    print("=" * 70)

    # Step 5: Delete (if requested)
    if auto_delete:
        print("\nProceeding with live deletion as requested (--auto-delete)...")
        run_delete(input_file=revalidate_file, dry_run=False, email_addr=target_account)
    else:
        print("\n👉 To preview deletions: make dry-run")
        print("👉 To permanently move confirmed emails to Gmail Trash: make delete\n")


def main():
    parser = argparse.ArgumentParser(
        description="Gmail AI Email Cleaner - Modular Multi-Step Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
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
    p_all = subparsers.add_parser("run-all", help="Execute entire pipeline end-to-end")
    p_all.add_argument("--limit", type=int, default=100)
    p_all.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first")
    p_all.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS)
    p_all.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_all.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH)
    p_all.add_argument("--reset-cursor", action="store_true")
    p_all.add_argument("--auto-delete", action="store_true", help="Automatically delete without pausing for review")
    p_all.add_argument("--email", type=str, default=None)

    args = parser.parse_args()

    if args.command == "fetch":
        run_fetch(limit=args.limit, direction=args.direction, output_file=args.output,
                  reset_cursor=args.reset_cursor, snippet_length=args.snippet_length, email_addr=args.email)
    elif args.command == "scan":
        run_scan(input_file=args.input, output_file=args.output, batch_size=args.batch_size,
                 workers=args.workers, email_addr=args.email)
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
                         reset_cursor=args.reset_cursor, auto_delete=args.auto_delete, email_addr=args.email)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
