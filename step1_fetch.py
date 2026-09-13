import argparse
import csv
from datetime import datetime
import email
import re
import sys
import time
from common import (
    GMAIL_USER,
    DEFAULT_SNIPPET_LENGTH,
    connect_imap,
    load_state,
    save_state,
    get_decoded_header,
    extract_body_snippet,
    generate_artifact_path,
)


def run_fetch(limit=100, direction="oldest-first", output_file=None, reset_cursor=False,
              snippet_length=DEFAULT_SNIPPET_LENGTH, email_addr=None):
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
    starred_count = 0
    reply_count = 0

    for idx, uid in enumerate(selected_uids, 1):
        uid_bytes = str(uid).encode("utf-8")
        # Fetch FLAGS and message body
        status, msg_data = mail.uid("fetch", uid_bytes, "(FLAGS BODY.PEEK[])")
        if status != "OK" or not msg_data:
            continue

        raw_flags = b""
        msg = None

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                raw_flags = response_part[0]
                try:
                    msg = email.message_from_bytes(response_part[1])
                except Exception as e:
                    print(f"⚠️ Error parsing message UID {uid}: {e}")

        if not msg:
            continue

        try:
            sender = get_decoded_header(msg, "From")
            subject = get_decoded_header(msg, "Subject")
            date_str = get_decoded_header(msg, "Date")
            message_id = (msg.get("Message-ID") or "").strip()
            snippet = extract_body_snippet(msg, max_chars=snippet_length)
        except Exception as extract_err:
            print(f"⚠️ Warning: Could not parse email UID {uid}: {extract_err}")
            continue

        is_starred = b"\\Flagged" in raw_flags
        has_reply = bool(msg.get("In-Reply-To") or msg.get("References"))

        if is_starred:
            starred_count += 1
        if has_reply:
            reply_count += 1

        fetched_rows.append({
            "uid": str(uid),
            "message_id": message_id,
            "date": date_str,
            "from": sender,
            "subject": subject,
            "snippet": snippet,
            "is_starred": "TRUE" if is_starred else "FALSE",
            "is_reply": "TRUE" if has_reply else "FALSE"
        })

        if idx % 50 == 0 or idx == len(selected_uids):
            print(f"   Fetched [{idx}/{len(selected_uids)}] emails...")

    mail.close()
    mail.logout()

    if not fetched_rows:
        print("⚠️ No emails were successfully fetched.")
        return None

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
    print(f"📁 Output Artifact Saved : {output_file}\n")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Step 1: Fetch emails via single safe IMAP connection")
    parser.add_argument("--limit", type=int, default=100, help="Maximum number of emails to fetch (default: 100)")
    parser.add_argument("--direction", choices=["oldest-first", "newest-first"], default="oldest-first", help="Fetch order")
    parser.add_argument("--snippet-length", type=int, default=DEFAULT_SNIPPET_LENGTH, help="Snippet length")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    parser.add_argument("--reset-cursor", action="store_true", help="Reset cursor in state.json")
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
