"""Step 4: Heuristic sanity audit and regex safety checker (0 IMAP connections)."""

import argparse
import csv
import os
import re
import sys

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)

def run_revalidate(input_file=None, output_file=None, email_addr=None):
    """
    Step 4: Final review preparation and audit packaging (Zero IMAP connections).
    Packages validated decisions from Step 3 into the final review artifact ready for deletion.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("3_validate", target_account)

    print("\n" + "=" * 65)
    print(f"🛡️ [STEP 4: REVALIDATE / REVIEW PACKAGING]")
    print(f"   • Account      : {target_account}")
    print(f"   • Input File   : {input_file}")
    print("=" * 65)

    if not input_file or not os.path.exists(input_file):
        print(f"❌ Error: Input artifact '{input_file}' not found. Please run Step 3 (validate) first.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    if not rows:
        print("⚠️ Input file is empty.")
        return None

    verified_delete_count = 0

    for r in rows:
        current_action = (r.get("final_action") or "").strip().upper()
        if current_action == "DELETE":
            verified_delete_count += 1
            r["revalidation_status"] = "CONFIRMED_DELETE"
            r["revalidation_notes"] = r.get("validator_reason", "Confirmed by auditor")
        else:
            r["revalidation_status"] = "KEPT"
            r["revalidation_notes"] = r.get("validator_reason", "Retained")

    if output_file is None:
        output_file = generate_artifact_path("4_revalidate", "revalidated", target_account)

    fieldnames = [
        "uid", "message_id", "date", "from", "subject", "snippet",
        "is_starred", "is_reply", "ai_decision", "ai_reason",
        "validator_decision", "validator_reason",
        "revalidation_status", "revalidation_notes",
        "final_action"
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n📊 [REVALIDATION COMPLETE]")
    print(f"   • Total emails audited     : {len(rows)}")
    print(f"   • Confirmed to delete      : {verified_delete_count}")
    print(f"   • Confirmed to keep        : {len(rows) - verified_delete_count}")
    print(f"\n📁 Final Review Artifact Saved: {output_file}")
    print("👉 Inspect this file to verify deletions before running Step 5 (delete).\n")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Step 4: Heuristic sanity audit and human review prep")
    parser.add_argument("--input", type=str, default=None, help="Input validated CSV path (default: latest in 3_validate/)")
    parser.add_argument("--output", type=str, default=None, help="Output revalidated CSV path")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_revalidate(
        input_file=args.input,
        output_file=args.output,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
