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
from gmail_cleaner.state import (
    load_state,
    save_state,
    generate_artifact_path,
)


def run_fetch(limit=100, direction="oldest-first", output_file=None, reset_cursor=False,
              snippet_length=DEFAULT_SNIPPET_LENGTH, email_addr=None,
              batch_size=50, conns=3):
    """
    Step 1: Connects to Gmail via ONE single safe IMAP connection, fetches headers/snippets,
    pre-protects Starred and Reply emails, and writes outputs/<email>/1_fetch/fetch_<timestamp>.csv.
    """
    target_account = email_addr or GMAIL_USER
    print("\n" + "=" * 65)
    print(f"📥 [STEP 1: FETCH EMAILS (SINGLE IMAP CONNECTION)]")
    print(f"   • Account       : {target_account}")
    print(f"   • Limit         : {limit} emails")
    print(f"   • Order         : {direction}")
    print(f"   • Snippet Length: {snippet_length} chars")
    print("=" * 65)

    state = load_state(target_account)
    if reset_cursor:
        print("🔄 Resetting cursor to start from the beginning.")
        state["last_processed_uid"] = 0

    print("Connecting to Gmail IMAP...")
    mail = connect_imap(email_user=target_account)
    mail.select("INBOX")

    # Verify UIDVALIDITY
    status, data = mail.status("INBOX", "(UIDVALIDITY)")
    current_validity = None
    if status == "OK" and data and data[0]:
        match = re.search(r"UIDVALIDITY\s+(\d+)", data[0].decode("utf-8", errors="ignore"))
        if match:
            current_validity = match.group(1)

    if state.get("uid_validity") and current_validity and state["uid_validity"] != current_validity:
        print("⚠️ Mailbox UIDVALIDITY changed. Resetting cursor for safety.")
        state["last_processed_uid"] = 0
    state["uid_validity"] = current_validity

    last_uid = state.get("last_processed_uid", 0)
    print(f"Current cursor: last_processed_uid = {last_uid}")

    # Query UIDs
    if direction == "oldest-first" and last_uid > 0:
        search_criteria = f"UID {last_uid + 1}:*"
        status, messages = mail.uid("search", None, search_criteria)
    else:
        status, messages = mail.uid("search", None, "ALL")

    if status != "OK" or not messages[0]:
        print("✅ No emails found matching search criteria.")
        mail.close()
        mail.logout()
        return None

    raw_uids = [int(x) for x in messages[0].split()]
    if direction == "oldest-first" and last_uid > 0:
        raw_uids = [uid for uid in raw_uids if uid > last_uid]

    if not raw_uids:
        print("✅ No new emails to process since last cursor.")
        mail.close()
        mail.logout()
        return None

    if direction == "oldest-first":
        raw_uids.sort()
    else:
        raw_uids.sort(reverse=True)

    selected_uids = raw_uids[:limit]
    print(f"Found {len(raw_uids)} candidates. Fetching next {len(selected_uids)} emails...")

    fetched_rows = []
    start_time = time.time()

    if conns > 1 and len(selected_uids) > batch_size:
        mail.close()
        mail.logout()
        print(f"Accelerating fetch with {conns} parallel IMAP connections (50/batch)...")

        chunk_size = (len(selected_uids) + conns - 1) // conns
        chunks = [selected_uids[i:i + chunk_size] for i in range(0, len(selected_uids), chunk_size)]

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
            print(f"   Fetched [{min(i + len(b), len(selected_uids))}/{len(selected_uids)}] emails...")
        mail.close()
        mail.logout()

    if not fetched_rows:
        print("⚠️ No emails were successfully fetched.")
        return None

    # Preserve exact UID order
    uid_order = {str(uid): idx for idx, uid in enumerate(selected_uids)}
    fetched_rows.sort(key=lambda r: uid_order.get(r["uid"], 0))

    starred_count = sum(1 for r in fetched_rows if r.get("is_starred") == "TRUE")
    reply_count = sum(1 for r in fetched_rows if r.get("is_reply") == "TRUE")

    if output_file is None:
        output_file = generate_artifact_path("1_fetch", "fetch", target_account)

    fieldnames = ["uid", "message_id", "date", "from", "subject", "snippet", "is_starred", "is_reply"]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(fetched_rows)

    max_uid = max(int(r["uid"]) for r in fetched_rows)
    if direction == "oldest-first":
        state["last_processed_uid"] = max(state.get("last_processed_uid", 0), max_uid)
    state["total_scanned"] = state.get("total_scanned", 0) + len(fetched_rows)
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state, target_account)

    elapsed = time.time() - start_time
    print(f"\n📊 [FETCH COMPLETE] in {elapsed:.2f}s")
    print(f"   • Fetched emails       : {len(fetched_rows)}")
    print(f"   • Starred (Auto-Keep)  : {starred_count}")
    print(f"   • Thread Replies (Keep): {reply_count}")
    print(f"   • Cursor updated to UID: {state['last_processed_uid']}")
    print(f"📁 Output Artifact Saved  : {output_file}\n")
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
