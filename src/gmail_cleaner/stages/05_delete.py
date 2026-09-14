import argparse
import csv
import os
import shutil
import sys

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.imap_client import connect_imap
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)

logger = get_logger("delete")


def run_delete(input_file=None, dry_run=False, email_addr=None):
    """
    Step 5: Reads revalidated artifact from 4_revalidate/, connects via ONE single IMAP connection,
    moves confirmed DELETE emails to Gmail Trash, and archives to outputs/<email>/5_processed/completed_<ts>.csv.
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)

    if not input_file:
        input_file = get_latest_artifact("4_revalidate", target_account)

    logger.info("=" * 65)
    logger.info("🗑️ [STEP 5: DELETE / TRASH EXECUTION]")
    logger.info(f"   • Account      : {target_account}")
    logger.info(f"   • Input File   : {input_file}")
    logger.info(f"   • Dry Run Mode : {dry_run}")
    logger.info("=" * 65)

    if not input_file or not os.path.exists(input_file):
        logger.error(f"❌ Error: Input artifact '{input_file}' not found. Please run Step 4 (revalidate) first.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        logger.warning("⚠️ Input file is empty.")
        return None

    to_delete = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    to_keep = [r for r in rows if (r.get("final_action") or "").strip().upper() != "DELETE"]

    logger.info(f"   • Total emails evaluated : {len(rows)}")
    logger.info(f"   • Confirmed for DELETION : {len(to_delete)}")
    logger.info(f"   • Confirmed to KEEP      : {len(to_keep)}")

    if not to_delete:
        logger.info("✅ No emails marked with final_action = 'DELETE'. Nothing to trash!")
        return None

    if dry_run:
        logger.info("🔍 [DRY RUN PREVIEW] The following emails WOULD be moved to Trash:")
        for r in to_delete[:20]:
            logger.info(f"   ❌ UID {r['uid']} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        if len(to_delete) > 20:
            logger.info(f"   ... and {len(to_delete) - 20} more.")
        logger.info("No changes made in Gmail. Run without '--dry-run' to execute live deletion.")
        return None

    logger.info("Connecting to Gmail IMAP (Single Connection)...")
    mail = connect_imap(email_user=target_account)
    mail.select("INBOX")

    uids_to_trash = [r["uid"] for r in to_delete]
    TRASH_BATCH_SIZE = 50
    success_count = 0

    for i in range(0, len(uids_to_trash), TRASH_BATCH_SIZE):
        batch = uids_to_trash[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        logger.debug(f"Trashing batch of {len(batch)} UIDs (first={batch[0]}, last={batch[-1]})...")
        status, response = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Trash")
        if status == "OK":
            success_count += len(batch)
            logger.debug(f"Trashing progress: {success_count}/{len(uids_to_trash)} emails moved to Trash")
        else:
            logger.warning(f"⚠️ Warning: Could not trash UID batch: {uid_set} (status={status})")

    mail.expunge()
    mail.close()
    mail.logout()

    logger.info(f"🎉 Successfully moved {success_count} emails to Gmail Trash!")

    # Update SQLite database if available
    try:
        from gmail_cleaner.db import EmailDB
        db = EmailDB(account=target_account)
        db.mark_trashed(uids_to_trash[:success_count])
    except Exception as e:
        logger.warning(f"Could not update SQLite DB with TRASHED status: {e}")

    # Archive completed review file
    completed_path = generate_artifact_path("5_processed", "completed", target_account)
    try:
        shutil.copyfile(input_file, completed_path)
        logger.info(f"📦 Archived execution record to: {completed_path}")
    except Exception as e:
        logger.error(f"⚠️ Could not archive file: {e}", exc_info=True)

    return completed_path


def main():
    parser = argparse.ArgumentParser(description="Step 5: Apply deletion of confirmed emails to Gmail Trash")
    parser.add_argument("--input", type=str, default=None, help="Input revalidated CSV path (default: latest in 4_revalidate/)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate deletion without touching Gmail")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_delete(
        input_file=args.input,
        dry_run=args.dry_run,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
