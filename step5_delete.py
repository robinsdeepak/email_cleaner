import argparse
import csv
import os
import shutil
import sys
from common import (
    GMAIL_USER,
    connect_imap,
    get_latest_artifact,
    generate_artifact_path,
)


def run_delete(input_file=None, dry_run=False, email_addr=None):
    """
    Step 5: Reads revalidated artifact from 4_revalidate/, connects via ONE single IMAP connection,
    moves confirmed DELETE emails to Gmail Trash, and archives to outputs/<email>/5_processed/completed_<ts>.csv.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("4_revalidate", target_account)

    print("\n" + "=" * 65)
    print(f"🗑️ [STEP 5: DELETE / TRASH EXECUTION]")
    print(f"   • Account      : {target_account}")
    print(f"   • Input File   : {input_file}")
    print(f"   • Dry Run Mode : {dry_run}")
    print("=" * 65)

    if not input_file or not os.path.exists(input_file):
        print(f"❌ Error: Input artifact '{input_file}' not found. Please run Step 4 (revalidate) first.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        print("⚠️ Input file is empty.")
        return None

    to_delete = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    to_keep = [r for r in rows if (r.get("final_action") or "").strip().upper() != "DELETE"]

    print(f"   • Total emails evaluated : {len(rows)}")
    print(f"   • Confirmed for DELETION : {len(to_delete)}")
    print(f"   • Confirmed to KEEP      : {len(to_keep)}")

    if not to_delete:
        print("✅ No emails marked with final_action = 'DELETE'. Nothing to trash!")
        return None

    if dry_run:
        print("\n🔍 [DRY RUN PREVIEW] The following emails WOULD be moved to Trash:")
        for r in to_delete[:20]:
            print(f"   ❌ UID {r['uid']} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        if len(to_delete) > 20:
            print(f"   ... and {len(to_delete) - 20} more.")
        print("\nNo changes made in Gmail. Run without '--dry-run' to execute live deletion.")
        return None

    print("\nConnecting to Gmail IMAP (Single Connection)...")
    mail = connect_imap(email_user=target_account)
    mail.select("INBOX")

    uids_to_trash = [r["uid"] for r in to_delete]
    TRASH_BATCH_SIZE = 50
    success_count = 0

    for i in range(0, len(uids_to_trash), TRASH_BATCH_SIZE):
        batch = uids_to_trash[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        status, response = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Trash")
        if status == "OK":
            success_count += len(batch)
        else:
            print(f"⚠️ Warning: Could not trash UID batch: {uid_set}")

    mail.expunge()
    mail.close()
    mail.logout()

    print(f"🎉 Successfully moved {success_count} emails to Gmail Trash!")

    # Archive completed review file
    completed_path = generate_artifact_path("5_processed", "completed", target_account)
    try:
        shutil.copyfile(input_file, completed_path)
        print(f"📦 Archived execution record to: {completed_path}\n")
    except Exception as e:
        print(f"⚠️ Could not archive file: {e}")

    return completed_path


def main():
    parser = argparse.ArgumentParser(description="Step 5: Apply deletion of confirmed emails to Gmail Trash")
    parser.add_argument("--input", type=str, default=None, help="Input revalidate CSV path (default: latest in 4_revalidate/)")
    parser.add_argument("--dry-run", action="store_true", help="Simulate deletion without modifying Gmail")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_delete(
        input_file=args.input,
        dry_run=args.dry_run,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
