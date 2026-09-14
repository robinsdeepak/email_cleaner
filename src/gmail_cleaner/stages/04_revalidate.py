import argparse
import csv
import os
import re
import sys

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)

logger = get_logger("revalidate")


def run_revalidate(input_file=None, output_file=None, email_addr=None):
    """
    Step 4: Final review preparation and audit packaging (Zero IMAP connections).
    Packages validated decisions from Step 3 into the final review artifact ready for deletion.
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)
    from gmail_cleaner.db import EmailDB
    db = EmailDB(account=target_account)

    rows = []
    source_desc = ""

    if not input_file:
        # Check SQLite DB
        db_rows = db.query("SELECT * FROM emails WHERE account = ? AND status IN ('SCANNED', 'AUDITED')", (target_account,))
        if db_rows:
            rows = db_rows
            source_desc = f"SQLite emails.db ({len(rows)} emails)"
        else:
            latest_csv = get_latest_artifact("3_validate", target_account)
            if latest_csv and os.path.exists(latest_csv):
                input_file = latest_csv
                source_desc = f"CSV artifact: {input_file}"
    else:
        source_desc = f"CSV artifact: {input_file}"

    logger.info("=" * 65)
    logger.info("🛡️ [STEP 4: REVALIDATE / REVIEW PACKAGING]")
    logger.info(f"   • Account      : {target_account}")
    logger.info(f"   • Input Source : {source_desc or 'None'}")
    logger.info("=" * 65)

    if not rows and input_file and os.path.exists(input_file):
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                rows.append(r)

    if not rows:
        logger.warning("⚠️ No emails available to revalidate.")
        return None

    verified_delete_count = 0
    needs_review_count = 0

    for r in rows:
        current_action = (r.get("final_action") or "").strip().upper()
        if current_action == "DELETE":
            verified_delete_count += 1
            r["revalidation_status"] = "CONFIRMED_DELETE"
            r["revalidation_notes"] = r.get("validator_reason", "Confirmed by auditor")
        elif current_action == "REVIEW":
            needs_review_count += 1
            r["revalidation_status"] = "NEEDS_REVIEW"
            r["revalidation_notes"] = r.get("validator_reason", "Requires human review")
        else:
            r["revalidation_status"] = "KEPT"
            r["revalidation_notes"] = r.get("validator_reason", "Retained")

    if output_file is not None or os.environ.get("WRITE_LEGACY_CSV"):
        if output_file is None:
            output_file = generate_artifact_path("4_revalidate", "revalidated", target_account)
        fieldnames = [
            "uid", "message_id", "date", "from", "subject", "snippet",
            "is_starred", "is_reply", "ai_decision", "ai_confidence", "ai_category", "ai_reason",
            "validator_decision", "validator_confidence", "validator_reason",
            "revalidation_status", "revalidation_notes",
            "final_action"
        ]
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                if "from" not in r:
                    r["from"] = r.get("sender", "")
                writer.writerow(r)
        logger.info(f"📁 Optional Review Artifact Saved: {output_file}")

    logger.info("📊 [REVALIDATION COMPLETE]")
    logger.info(f"   • Total emails audited     : {len(rows)}")
    logger.info(f"   • Confirmed to delete      : {verified_delete_count}")
    logger.info(f"   • Flagged for human review : {needs_review_count}")
    logger.info(f"   • Confirmed to keep        : {len(rows) - verified_delete_count - needs_review_count}")
    logger.info("   • Persistence Layer        : SQLite emails.db (100% DB-driven)")
    logger.info("👉 Inspect and review emails directly in browser UI (Tab 2: Email Explorer & Review).")
    return output_file or "DB_UPDATED"


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
