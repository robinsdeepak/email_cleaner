"""Step 1: Fetch emails via a single dedicated IMAP connection."""

import argparse
import concurrent.futures
import csv
from datetime import datetime
import email
import re
import sys
import time

from gmail_cleaner.config import GMAIL_USER, DEFAULT_SNIPPET_LENGTH
from gmail_cleaner.imap_client import (
    connect_imap,
    fetch_batch_uids_fast,
)
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import generate_artifact_path

logger = get_logger("fetch")


def run_fetch(limit=100, direction="oldest-first", output_file=None, reset_cursor=False,
              snippet_length=DEFAULT_SNIPPET_LENGTH, email_addr=None,
              batch_size=50, conns=3, run_id=None):
    """
    Step 1: Connects to Gmail via ONE single safe IMAP connection, fetches headers/snippets,
    pre-protects Starred and Reply emails directly into SQLite (Zero loose state files).
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)
    from gmail_cleaner.db import EmailDB
    db = EmailDB(account=target_account)

    if not run_id:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{timestamp}_fetch"
        db.create_run(
            action_type="Fetch Emails",
            run_id=run_id,
            params={"limit": limit, "direction": direction, "conns": conns, "snippet_length": snippet_length},
        )

    logger.info("=" * 65)
    logger.info("📥 [STEP 1: FETCH EMAILS (SINGLE IMAP CONNECTION)]")
    logger.info(f"   • Account       : {target_account}")
    logger.info(f"   • Limit         : {limit} emails")
    logger.info(f"   • Order         : {direction}")
    logger.info(f"   • Snippet Length: {snippet_length} chars")
    logger.info(f"   • Run ID        : {run_id}")
    logger.info("=" * 65)

    if reset_cursor:
        logger.info("🔄 Resetting cursor to start from the beginning.")
        db.update_account_cursor(target_account, 0)

    last_uid = 0 if reset_cursor else db.get_account_cursor(target_account)
    logger.info(f"Current cursor: last_processed_uid = {last_uid}")

    logger.info("Connecting to Gmail IMAP...")
    mail = connect_imap(email_user=target_account)
    mail.select("INBOX")

    # Verify mailbox UIDVALIDITY stored in SQLite
    status, data = mail.status("INBOX", "(UIDVALIDITY)")
    current_validity = None
    if status == "OK" and data and data[0]:
        match = re.search(r"UIDVALIDITY\s+(\d+)", data[0].decode("utf-8", errors="ignore"))
        if match:
            current_validity = match.group(1)

    acc_info = db.get_account(target_account)
    stored_validity = acc_info.get("uid_validity") if acc_info else None
    if stored_validity and current_validity and stored_validity != current_validity:
        logger.warning("⚠️ Mailbox UIDVALIDITY changed. Resetting cursor in DB for safety.")
        last_uid = 0
        db.update_account_cursor(target_account, 0, uid_validity=current_validity)
    elif current_validity and current_validity != stored_validity:
        db.update_account_cursor(target_account, last_uid, uid_validity=current_validity)

    # Query UIDs
    if direction == "oldest-first" and last_uid > 0:
        search_criteria = f"UID {last_uid + 1}:*"
        status, messages = mail.uid("search", None, search_criteria)
    else:
        status, messages = mail.uid("search", None, "ALL")

    if status != "OK" or not messages[0]:
        logger.info("✅ No emails found matching search criteria.")
        mail.close()
        mail.logout()
        return None

    raw_uids = [int(x) for x in messages[0].split()]
    if direction == "oldest-first" and last_uid > 0:
        raw_uids = [uid for uid in raw_uids if uid > last_uid]

    if not raw_uids:
        logger.info("✅ No new emails to process since last cursor.")
        mail.close()
        mail.logout()
        return None

    if direction == "oldest-first":
        raw_uids.sort()
    else:
        raw_uids.sort(reverse=True)

    selected_uids = raw_uids[:limit]
    logger.info(f"Found {len(raw_uids)} candidates. Fetching next {len(selected_uids)} emails...")
    logger.debug(f"UID range selected: {selected_uids[0]}..{selected_uids[-1]}")

    fetched_rows = []
    start_time = time.time()

    if conns > 1 and len(selected_uids) > batch_size:
        mail.close()
        mail.logout()
        logger.info(f"Accelerating fetch with {conns} parallel IMAP connections (50/batch)...")

        chunk_size = (len(selected_uids) + conns - 1) // conns
        chunks = [selected_uids[i:i + chunk_size] for i in range(0, len(selected_uids), chunk_size)]
        logger.debug(f"Partitioned {len(selected_uids)} UIDs into {len(chunks)} connection slices.")

        def worker_fetch_chunk(uid_chunk):
            conn = connect_imap(email_user=target_account)
            conn.select("INBOX")
            chunk_rows = []
            for i in range(0, len(uid_chunk), batch_size):
                b = uid_chunk[i:i + batch_size]
                chunk_rows.extend(fetch_batch_uids_fast(conn, b, snippet_length=snippet_length))
            conn.close()
            conn.logout()
            return chunk_rows

        with concurrent.futures.ThreadPoolExecutor(max_workers=conns) as executor:
            future_results = executor.map(worker_fetch_chunk, chunks)
            for res in future_results:
                fetched_rows.extend(res)
    else:
        for i in range(0, len(selected_uids), batch_size):
            b = selected_uids[i:i + batch_size]
            fetched_rows.extend(fetch_batch_uids_fast(mail, b, snippet_length=snippet_length))
            logger.info(f"   Fetched [{min(i + len(b), len(selected_uids))}/{len(selected_uids)}] emails...")
        mail.close()
        mail.logout()

    if not fetched_rows:
        logger.warning("⚠️ No emails were successfully fetched.")
        return None

    # Preserve exact UID order
    uid_order = {str(uid): idx for idx, uid in enumerate(selected_uids)}
    fetched_rows.sort(key=lambda r: uid_order.get(r["uid"], 0))

    for r in fetched_rows:
        r["last_run_id"] = run_id

    starred_count = sum(1 for r in fetched_rows if r.get("is_starred") == "TRUE")
    reply_count = sum(1 for r in fetched_rows if r.get("is_reply") == "TRUE")

    # Upsert directly into SQLite (Zero CSV dependency)
    db.upsert_emails(fetched_rows)
    db.update_run(run_id, status="COMPLETED", total_emails=len(fetched_rows))

    if output_file is not None or os.environ.get("WRITE_LEGACY_CSV"):
        if output_file is None:
            output_file = generate_artifact_path("1_fetch", "fetch", target_account)
        fieldnames = ["uid", "message_id", "date", "from", "subject", "snippet", "is_starred", "is_reply"]
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(fetched_rows)
        logger.info(f"📁 Optional Artifact Saved : {output_file}")

    max_uid = max(int(r["uid"]) for r in fetched_rows)
    new_cursor = max(last_uid, max_uid) if direction == "oldest-first" else last_uid
    acc_stat = db.get_stats()
    db.update_account_cursor(target_account, new_cursor, acc_stat.get("total_emails", len(fetched_rows)))

    elapsed = time.time() - start_time
    logger.info(f"📊 [FETCH COMPLETE] in {elapsed:.2f}s")
    logger.info(f"   • Fetched emails       : {len(fetched_rows)}")
    logger.info(f"   • Starred (Auto-Keep)  : {starred_count}")
    logger.info(f"   • Thread Replies (Keep): {reply_count}")
    logger.info(f"   • Cursor updated to UID: {new_cursor}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Step 1: Fetch emails via single safe IMAP connection")
    parser.add_argument("--limit", type=int, default=100, help="Number of emails to fetch")
    parser.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first", help="Fetch direction")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    parser.add_argument("--reset-cursor", action="store_true", help="Reset state cursor to 0")
    parser.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH, help="Max snippet length")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_fetch(
        limit=args.limit,
        direction=args.direction,
        output_file=args.output,
        reset_cursor=args.reset_cursor,
        snippet_length=args.snippet_length,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
