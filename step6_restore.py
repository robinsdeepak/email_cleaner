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


def run_restore(input_file=None, dry_run=False, email_addr=None):
    r"""
    Step 6: Undo / Restore previously deleted emails from Gmail Trash back to Inbox.
    Reads an execution archive from 5_processed/, removes \Trash label, and re-adds \Inbox.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("5_processed", target_account, pattern="completed_*.csv")

    print("\n" + "=" * 65)
    print(f"🔄 [STEP 6: UNDO / RESTORE DELETED EMAILS]")
    print(f"   • Account      : {target_account}")
    print(f"   • Input File   : {input_file}")
    print(f"   • Dry Run Mode : {dry_run}")
    print("=" * 65)

    if not input_file or not os.path.exists(input_file):
        print(f"❌ Error: Processed archive file '{input_file}' not found in 5_processed/.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    emails_to_restore = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    print(f"   • Total emails in archive : {len(rows)}")
    print(f"   • Emails to RESTORE       : {len(emails_to_restore)}")

    if not emails_to_restore:
        print("✅ No deleted emails found in this archive file.")
        return None

    if dry_run:
        print("\n🔍 [DRY RUN PREVIEW] The following emails WOULD be restored to Inbox:")
        for r in emails_to_restore[:20]:
            print(f"   ↩️ UID {r.get('uid')} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        if len(emails_to_restore) > 20:
            print(f"   ... and {len(emails_to_restore) - 20} more.")
        print("\nNo changes made in Gmail. Run without '--dry-run' to execute restoration.")
        return None

    print("\nConnecting to Gmail Trash folder...")
    mail = connect_imap(email_user=target_account)

    trash_folder = "[Gmail]/Trash"
    status, _ = mail.select(trash_folder)
    if status != "OK":
        trash_folder = "[Gmail]/Bin"
        status, _ = mail.select(trash_folder)

    if status != "OK":
        print("❌ Error: Could not access Gmail Trash folder.")
        mail.logout()
        return None

    trash_uids_to_restore = []
    not_found = []

    print(f"Locating {len(emails_to_restore)} emails in {trash_folder} via Message-ID...")
    for idx, r in enumerate(emails_to_restore, 1):
        msg_id = (r.get("message_id") or "").strip()
        trash_uid = None

        # 1. Try search by RFC 822 Message-ID header
        if msg_id:
            clean_id = msg_id.strip("<>")
            try:
                # Use Gmail raw search first as it's fastest and exact
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
        else:
            not_found.append(r)

        if idx % 50 == 0 or idx == len(emails_to_restore):
            print(f"   Located [{len(trash_uids_to_restore)}/{idx}] emails in Trash...")

    if not trash_uids_to_restore:
        print("\n⚠️ None of the target emails could be located in Trash (they may have been permanently emptied).")
        mail.close()
        mail.logout()
        return None

    TRASH_BATCH_SIZE = 50
    restored_count = 0

    print(f"\nRestoring {len(trash_uids_to_restore)} emails from Trash back to Inbox...")
    for i in range(0, len(trash_uids_to_restore), TRASH_BATCH_SIZE):
        batch = trash_uids_to_restore[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        mail.uid("store", uid_set, "-X-GM-LABELS", "\\Trash")
        status, _ = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Inbox")
        if status == "OK":
            restored_count += len(batch)
        else:
            print(f"⚠️ Warning: Could not untrash UID batch: {uid_set}")

    mail.close()
    mail.logout()

    print(f"\n🎉 Successfully restored {restored_count} emails back to your Inbox!")
    if not_found:
        print(f"ℹ️ {len(not_found)} emails could not be located in Trash (may already be deleted or restored).")

    # Mark the archive file as restored
    restored_path = generate_artifact_path("5_processed", "restored", target_account)
    try:
        shutil.move(input_file, restored_path)
        print(f"📦 Renamed archive to: {restored_path}\n")
    except Exception as e:
        print(f"⚠️ Could not rename archive: {e}")

    return restored_path


def main():
    parser = argparse.ArgumentParser(description="Step 6: Undo deletion and restore emails from Trash back to Inbox")
    parser.add_argument("--input", type=str, default=None, help="Input completed CSV path (default: latest in 5_processed/)")
    parser.add_argument("--dry-run", action="store_true", help="Preview restore without modifying Gmail")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_restore(
        input_file=args.input,
        dry_run=args.dry_run,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
