import argparse
import csv
import os
import shutil
import sys
import time
from typing import Optional, List

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.db import EmailDB
from gmail_cleaner.imap_client import connect_imap
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)
from gmail_cleaner.worker import worker

logger = get_logger("delete")


def run_delete(input_file=None, dry_run=False, email_addr=None, run_id: Optional[str] = None,
               statuses: Optional[List[str]] = None):
    """
    Step 5: Database-first deletion execution.
    If input_file is not provided, reads confirmed DELETE emails directly from SQLite (honoring manual UI overrides).
    Can filter candidate deletions by specific status categories (e.g. CONFIDENT_DELETE, PROBABLE_DELETE, NEEDS_REVIEW).
    Connects via single IMAP connection, moves confirmed emails to Gmail Trash,
    archives snapshot to outputs/<email>/5_processed/completed_<ts>.csv, and updates runs table.
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)
    db = EmailDB(account=target_account)

    if not run_id:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{timestamp}_{'dryrun' if dry_run else 'delete'}"
    act_name = "Deletion Dry Run" if dry_run else "Move to Trash"
    db.create_run(
        action_type=act_name,
        run_id=run_id,
        params={"dry_run": dry_run, "statuses": statuses},
    )

    logger.info("=" * 65)
    logger.info("🗑️ [STEP 5: DELETE / TRASH EXECUTION]")
    logger.info(f"   • Account       : {target_account}")
    logger.info(f"   • Run ID        : {run_id}")
    if statuses:
        logger.info(f"   • Target Status : {', '.join(statuses)}")
    logger.info(f"   • Input Source  : {'Database (emails.db)' if not input_file else input_file}")
    logger.info(f"   • Dry Run Mode  : {dry_run}")
    logger.info("=" * 65)

    to_delete = []
    total_evaluated = 0

    if not input_file:
        # Database-first mode: honors all manual overrides and selected statuses in UI
        db_deletions = db.get_confirmed_deletions(statuses=statuses)
        if db_deletions:
            to_delete = db_deletions
            stats = db.get_stats()
            total_evaluated = stats.get("total_emails", len(to_delete))
            logger.info(f"✅ Loaded {len(to_delete)} confirmed deletions matching statuses {statuses or 'ALL'} directly from SQLite")
        else:
            # Fall back to latest CSV artifact if DB has no pending deletions
            latest_csv = get_latest_artifact("4_revalidate", target_account)
            if latest_csv and os.path.exists(latest_csv):
                input_file = latest_csv
                logger.info(f"Falling back to latest revalidation CSV: {input_file}")

    if input_file and os.path.exists(input_file) and not to_delete:
        rows = []
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
        total_evaluated = len(rows)
        to_delete = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]

    if not to_delete:
        logger.info("✅ No emails marked with final_action = 'DELETE'. Nothing to trash!")
        if run_id:
            db.update_run(run_id, status="COMPLETED", total_emails=total_evaluated, delete_count=0, trashed_count=0)
        return None

    logger.info(f"   • Total emails evaluated : {total_evaluated}")
    logger.info(f"   • Confirmed for DELETION : {len(to_delete)}")
    logger.info(f"   • Confirmed to KEEP      : {max(0, total_evaluated - len(to_delete))}")

    if dry_run:
        logger.info("🔍 [DRY RUN PREVIEW] The following emails WOULD be moved to Trash:")
        for r in to_delete[:20]:
            sender = r.get("from") or r.get("sender") or ""
            subj = r.get("subject") or ""
            logger.info(f"   ❌ UID {r['uid']} | {sender[:25]} | {subj[:45]}")
        if len(to_delete) > 20:
            logger.info(f"   ... and {len(to_delete) - 20} more.")
        logger.info("No changes made in Gmail. Run without '--dry-run' to execute live deletion.")
        if run_id:
            db.update_run(
                run_id,
                status="COMPLETED",
                total_emails=total_evaluated,
                delete_count=len(to_delete),
                trashed_count=0,
                notes="Dry-run simulation completed; zero changes made in Gmail",
            )
        return None

    logger.info("Connecting to Gmail IMAP (Single Connection)...")
    mail = connect_imap(email_user=target_account)
    mail.select("INBOX")

    uids_to_trash = [str(r["uid"]) for r in to_delete]
    TRASH_BATCH_SIZE = 50
    success_count = 0
    total_to_trash = len(uids_to_trash)

    worker.update_progress(0, total_to_trash, f"Trashing: 0/{total_to_trash}")

    for i in range(0, total_to_trash, TRASH_BATCH_SIZE):
        if worker.is_cancel_requested:
            logger.warning("Trashing operation cancelled by user.")
            break

        batch = uids_to_trash[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        logger.debug(f"Trashing batch of {len(batch)} UIDs (first={batch[0]}, last={batch[-1]})...")
        status, response = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Trash")
        if status == "OK":
            success_count += len(batch)
            worker.update_progress(success_count, total_to_trash, f"Trashing: {success_count}/{total_to_trash}")
            logger.debug(f"Trashing progress: {success_count}/{total_to_trash} emails moved to Trash")
        else:
            logger.warning(f"⚠️ Warning: Could not trash UID batch: {uid_set} (status={status})")

    mail.expunge()
    mail.close()
    mail.logout()

    logger.info(f"🎉 Successfully moved {success_count} emails to Gmail Trash!")

    # Mark as TRASHED in SQLite
    trashed_uids = uids_to_trash[:success_count]
    db.mark_trashed(trashed_uids, run_id=run_id)

    # Archive execution snapshot
    completed_path = generate_artifact_path("5_processed", "completed", target_account)
    try:
        if input_file and os.path.exists(input_file):
            shutil.copyfile(input_file, completed_path)
        else:
            db.export_to_csv(completed_path, status="TRASHED")
        logger.info(f"📦 Archived execution record to: {completed_path}")
    except Exception as e:
        logger.error(f"⚠️ Could not archive file: {e}", exc_info=True)

    if run_id:
        db.update_run(
            run_id,
            status="COMPLETED",
            total_emails=total_evaluated,
            delete_count=len(to_delete),
            trashed_count=success_count,
            artifact_path=completed_path,
        )

    return completed_path


def main():
    parser = argparse.ArgumentParser(description="Step 5: Apply deletion of confirmed emails to Gmail Trash")
    parser.add_argument("--input", type=str, default=None, help="Input revalidated CSV path (default: latest in 4_revalidate/)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate deletion without touching Gmail")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    parser.add_argument("--run-id", type=str, default=None, help="Pipeline run ID")
    parser.add_argument("--statuses", nargs="*", default=None, help="Filter by statuses (e.g. CONFIDENT_DELETE PROBABLE_DELETE NEEDS_REVIEW MANUAL_DELETE)")
    args = parser.parse_args()

    run_delete(
        input_file=args.input,
        dry_run=args.dry_run,
        email_addr=args.email,
        run_id=args.run_id,
        statuses=args.statuses,
    )


if __name__ == "__main__":
    main()
