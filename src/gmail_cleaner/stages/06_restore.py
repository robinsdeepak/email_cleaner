"""Step 6: Undo deletion and restore emails from Trash back to Inbox."""

import argparse
import csv
import os
import shutil
import sys
import time
from typing import Optional

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.imap_client import connect_imap
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)

logger = get_logger("restore")


def run_restore(input_file=None, dry_run=False, email_addr=None, run_id: Optional[str] = None, uids: Optional[list] = None):
    r"""
    Step 6: Undo / Restore previously deleted emails from Gmail Trash back to Inbox.
    Reads an execution archive from 5_processed/ or SQLite emails.db, removes \Trash label, and re-adds \Inbox.
    Supports targeting specific UIDs for individual or group restoration.
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)
    from gmail_cleaner.db import EmailDB
    db = EmailDB(account=target_account)

    if not run_id:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        act_name = "Restore Dry Run" if dry_run else "Restore Emails"
        run_id = f"run_{timestamp}_{'dryrestore' if dry_run else 'restore'}"
        db.create_run(
            action_type=act_name,
            run_id=run_id,
            params={"dry_run": dry_run, "uids_count": len(uids) if uids else None},
        )

    emails_to_restore = []
    source_desc = ""

    if not input_file:
        # Check SQLite DB first
        if uids:
            int_uids = [int(u) for u in uids]
            placeholders = ",".join("?" for _ in int_uids)
            db_trashed = db.query(
                f"SELECT * FROM emails WHERE account = ? AND status = 'TRASHED' AND uid IN ({placeholders})",
                (target_account, *int_uids)
            )
            if db_trashed:
                emails_to_restore = db_trashed
                source_desc = f"SQLite emails.db ({len(emails_to_restore)} targeted trashed emails)"
        else:
            db_trashed = db.query("SELECT * FROM emails WHERE account = ? AND status = 'TRASHED'", (target_account,))
            if db_trashed:
                emails_to_restore = db_trashed
                source_desc = f"SQLite emails.db ({len(emails_to_restore)} trashed emails)"
        
        if not emails_to_restore:
            latest_csv = get_latest_artifact("5_processed", target_account, pattern="completed_*.csv")
            if latest_csv and os.path.exists(latest_csv):
                input_file = latest_csv
                source_desc = f"CSV archive: {input_file}"
    else:
        source_desc = f"CSV archive: {input_file}"

    logger.info("=" * 65)
    logger.info("🔄 [STEP 6: UNDO / RESTORE DELETED EMAILS]")
    logger.info(f"   • Account      : {target_account}")
    logger.info(f"   • Run ID       : {run_id}")
    logger.info(f"   • Input Source : {source_desc or 'None'}")
    logger.info(f"   • Dry Run Mode : {dry_run}")
    logger.info("=" * 65)

    if not emails_to_restore and input_file and os.path.exists(input_file):
        rows = []
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)
        emails_to_restore = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]

    if not emails_to_restore:
        logger.info("✅ No deleted emails found to restore.")
        return None

    logger.info(f"   • Emails to RESTORE       : {len(emails_to_restore)}")

    if not emails_to_restore:
        logger.info("✅ No deleted emails found in this archive file.")
        return None

    if dry_run:
        logger.info("🔍 [DRY RUN PREVIEW] The following emails WOULD be restored to Inbox:")
        for r in emails_to_restore[:20]:
            logger.info(f"   ↩️ UID {r.get('uid')} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        if len(emails_to_restore) > 20:
            logger.info(f"   ... and {len(emails_to_restore) - 20} more.")
        logger.info("No changes made in Gmail. Run without '--dry-run' to execute restoration.")
        return None

    logger.info("Connecting to Gmail Trash folder...")
    mail = connect_imap(email_user=target_account)

    trash_folder = "[Gmail]/Trash"
    status, _ = mail.select(trash_folder)
    if status != "OK":
        trash_folder = "[Gmail]/Bin"
        status, _ = mail.select(trash_folder)

    if status != "OK":
        logger.error("❌ Error: Could not access Gmail Trash folder.")
        mail.logout()
        return None

    trash_uids_to_restore = []
    not_found = []

    logger.info(f"Locating {len(emails_to_restore)} emails in {trash_folder} via Message-ID...")
    for idx, r in enumerate(emails_to_restore, 1):
        msg_id = (r.get("message_id") or "").strip()
        trash_uid = None

        # 1. Try search by RFC 822 Message-ID header
        if msg_id:
            clean_id = msg_id.strip("<>")
            try:
                status, data = mail.uid("search", None, "X-GM-RAW", f'rfc822msgid:{clean_id}')
                if status == "OK" and data and data[0]:
                    uids = data[0].decode("utf-8").split()
                    if uids:
                        trash_uid = uids[-1]
            except Exception:
                pass

            if not trash_uid:
                try:
                    status, data = mail.uid("search", None, f'HEADER Message-ID "{msg_id}"')
                    if status == "OK" and data and data[0]:
                        uids = data[0].decode("utf-8").split()
                        if uids:
                            trash_uid = uids[-1]
                except Exception:
                    pass

        # 2. Fallback to Subject search if Message-ID was not found
        if not trash_uid and r.get("subject"):
            subj = r["subject"].replace('"', '').strip()
            if subj:
                try:
                    status, data = mail.uid("search", None, f'SUBJECT "{subj[:50]}"')
                    if status == "OK" and data and data[0]:
                        uids = data[0].decode("utf-8").split()
                        if uids:
                            trash_uid = uids[-1]
                except Exception:
                    pass

        if trash_uid:
            trash_uids_to_restore.append(trash_uid)
            logger.debug(f"Located in trash: UID {r.get('uid')} -> Trash UID {trash_uid}")
        else:
            not_found.append(r)
            logger.debug(f"Could not locate in trash: UID {r.get('uid')} Message-ID: {msg_id}")

        if idx % 50 == 0 or idx == len(emails_to_restore):
            logger.info(f"   Located [{len(trash_uids_to_restore)}/{idx}] emails in Trash...")

    if not trash_uids_to_restore:
        logger.warning("⚠️ None of the target emails could be located in Trash (they may have been permanently emptied).")
        mail.close()
        mail.logout()
        return None

    TRASH_BATCH_SIZE = 50
    restored_count = 0

    logger.info(f"Restoring {len(trash_uids_to_restore)} emails from Trash back to Inbox...")
    for i in range(0, len(trash_uids_to_restore), TRASH_BATCH_SIZE):
        batch = trash_uids_to_restore[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        mail.uid("store", uid_set, "-X-GM-LABELS", "\\Trash")
        status, _ = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Inbox")
        if status == "OK":
            restored_count += len(batch)
            logger.debug(f"Restoration progress: {restored_count}/{len(trash_uids_to_restore)} restored to Inbox")
        else:
            logger.warning(f"⚠️ Warning: Could not untrash UID batch: {uid_set}")

    mail.close()
    mail.logout()

    logger.info(f"🎉 Successfully restored {restored_count} emails back to your Inbox!")
    if not_found:
        logger.info(f"ℹ️ {len(not_found)} emails could not be located in Trash (may already be deleted or restored).")

    # Update SQLite database if available
    try:
        from gmail_cleaner.db import EmailDB
        db = EmailDB(account=target_account)
        restored_uids = [r.get("uid") for r in emails_to_restore if r.get("uid")]
        db.mark_restored(restored_uids, run_id=run_id)
        if run_id:
            db.update_run(
                run_id,
                status="COMPLETED",
                total_emails=len(emails_to_restore),
                trashed_count=restored_count,
            )
    except Exception as e:
        logger.warning(f"Could not update SQLite DB with RESTORED status: {e}")

    # Mark the archive file as restored
    restored_path = generate_artifact_path("5_processed", "restored", target_account)
    try:
        if input_file and os.path.exists(input_file):
            shutil.move(input_file, restored_path)
            logger.info(f"📦 Renamed archive to: {restored_path}")
    except Exception as e:
        logger.error(f"⚠️ Could not rename archive: {e}", exc_info=True)

    return restored_path


def main():
    parser = argparse.ArgumentParser(description="Step 6: Undo deletion and restore emails from Trash back to Inbox")
    parser.add_argument("--input", type=str, default=None, help="Input completed CSV path (default: latest in 5_processed/)")
    parser.add_argument("--dry-run", action="store_true", help="Preview restore without modifying Gmail")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    parser.add_argument("--run-id", type=str, default=None, help="Pipeline run ID")
    args = parser.parse_args()

    run_restore(
        input_file=args.input,
        dry_run=args.dry_run,
        email_addr=args.email,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
