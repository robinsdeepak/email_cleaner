"""Step 2: Classify emails with Gemini AI (0 IMAP connections)."""

import argparse
import concurrent.futures
import csv
import os
import sys
import time

from gmail_cleaner.config import (
    GMAIL_USER,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
)
from gmail_cleaner.ai import (
    get_genai_client,
    classify_batch_with_gemini,
)
from gmail_cleaner.logger import get_logger, setup_logger
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)

logger = get_logger("scan")


def run_scan(input_file=None, output_file=None, batch_size=DEFAULT_BATCH_SIZE,
             workers=DEFAULT_MAX_WORKERS, email_addr=None, only_kept=False):
    """
    Step 2: Reads fetched artifact from 1_fetch/, runs parallel Gemini classification
    (Zero IMAP connections), and writes outputs/<email>/2_scan/scanned_<timestamp>.csv.
    """
    target_account = email_addr or GMAIL_USER
    setup_logger(email_addr=target_account)
    from gmail_cleaner.db import EmailDB
    db = EmailDB(account=target_account)

    rows = []
    source_desc = ""
    if not input_file:
        # Pure Database Mode
        db_rows = db.get_emails_for_scan(only_kept=only_kept)
        if db_rows:
            rows = db_rows
            source_desc = f"SQLite emails.db ({len(rows)} emails)"
        else:
            latest_csv = get_latest_artifact("1_fetch", target_account)
            if latest_csv and os.path.exists(latest_csv):
                input_file = latest_csv
                source_desc = f"CSV artifact: {input_file}"
    else:
        source_desc = f"CSV artifact: {input_file}"

    logger.info("=" * 65)
    logger.info("🤖 [STEP 2: SCAN & CLASSIFY (GEMINI AI)]")
    logger.info(f"   • Account      : {target_account}")
    logger.info(f"   • Input Source : {source_desc or 'None'}")
    logger.info(f"   • Concurrency  : {workers} workers")
    logger.info(f"   • Batch Size   : {batch_size} emails/request")
    if only_kept:
        logger.info("   • Filter       : Only scanning emails previously marked as KEEP")
    logger.info("=" * 65)

    if not rows and input_file and os.path.exists(input_file):
        with open(input_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                if only_kept and (r.get("final_action") or "").strip().upper() == "DELETE":
                    continue
                rows.append(r)

    if not rows:
        logger.info("✅ No emails pending classification (or no candidates match filter).")
        return None

    logger.info(f"Loaded {len(rows)} emails to classify.")

    # Pre-filter Starred and Reply emails
    ai_candidates = []
    final_results = []
    skipped_count = 0

    for r in rows:
        is_starred = str(r.get("is_starred", "FALSE")).upper() in ("TRUE", "1")
        is_reply = str(r.get("is_reply", "FALSE")).upper() in ("TRUE", "1")

        if is_starred:
            skipped_count += 1
            r["ai_decision"] = "CONFIDENT_KEEP"
            r["ai_confidence"] = "HIGH"
            r["ai_category"] = "PERSONAL"
            r["ai_reason"] = "Auto-Protected: Starred Email"
            r["final_action"] = "KEEP"
            final_results.append(r)
        elif is_reply:
            skipped_count += 1
            r["ai_decision"] = "CONFIDENT_KEEP"
            r["ai_confidence"] = "HIGH"
            r["ai_category"] = "PERSONAL"
            r["ai_reason"] = "Auto-Protected: Thread Reply / Reference"
            r["final_action"] = "KEEP"
            final_results.append(r)
        else:
            ai_candidates.append(r)

    logger.info(f"Auto-protected {skipped_count} Starred/Reply emails (0 tokens used).")
    logger.info(f"Sending {len(ai_candidates)} emails to Gemini for classification...")

    if ai_candidates:
        ai_client = get_genai_client()
        chunks = [ai_candidates[i:i + batch_size] for i in range(0, len(ai_candidates), batch_size)]
        start_time = time.time()
        eval_map = {}

        def process_chunk(chunk):
            payload = [
                {
                    "id": str(item["uid"]),
                    "date": item.get("date", ""),
                    "from": item.get("from") or item.get("sender", ""),
                    "subject": item.get("subject", ""),
                    "snippet": item.get("snippet", "")
                }
                for item in chunk
            ]
            return classify_batch_with_gemini(ai_client, payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_chunk = {executor.submit(process_chunk, chunk): chunk for chunk in chunks}
            completed_chunks = 0
            for future in concurrent.futures.as_completed(future_to_chunk):
                completed_chunks += 1
                try:
                    results = future.result()
                    for res in results:
                        if res.get("id"):
                            eval_map[str(res.get("id"))] = res
                except Exception as exc:
                    logger.error(f"⚠️ Batch generated an exception: {exc}", exc_info=True)
                logger.info(f"   Progress: [{completed_chunks}/{len(chunks)}] batches classified...")

        for item in ai_candidates:
            uid = str(item["uid"])
            evaluated = eval_map.get(uid, {})
            status_val = (evaluated.get("status") or evaluated.get("ai_decision") or "NEEDS_REVIEW").strip().upper()
            conf_val = (evaluated.get("confidence") or "MEDIUM").strip().upper()
            cat_val = (evaluated.get("category") or "OTHER").strip().upper()
            reason = evaluated.get("reason", "Unclassified")

            if status_val in ("CONFIDENT_DELETE", "PROBABLE_DELETE"):
                action = "DELETE"
            elif status_val in ("CONFIDENT_KEEP", "PROBABLE_KEEP"):
                action = "KEEP"
            else:
                action = "REVIEW"

            item["ai_decision"] = status_val
            item["ai_confidence"] = conf_val
            item["ai_category"] = cat_val
            item["ai_reason"] = reason
            item["final_action"] = action
            final_results.append(item)

        elapsed = time.time() - start_time
        logger.info(f"Classified {len(ai_candidates)} emails in {elapsed:.2f}s.")

    # Sort to preserve original UID order
    uid_order = {str(r["uid"]): idx for idx, r in enumerate(rows)}
    final_results.sort(key=lambda x: uid_order.get(str(x["uid"]), 0))

    # Persist directly into SQLite DB
    db.update_scan_batch(final_results)

    if output_file is not None or os.environ.get("WRITE_LEGACY_CSV"):
        if output_file is None:
            output_file = generate_artifact_path("2_scan", "scanned", target_account)
        fieldnames = ["uid", "message_id", "date", "from", "subject", "snippet", "is_starred", "is_reply", "ai_decision", "ai_confidence", "ai_category", "ai_reason", "final_action"]
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
            writer.writeheader()
            for item in final_results:
                if "from" not in item:
                    item["from"] = item.get("sender", "")
                writer.writerow(item)
        logger.info(f"📁 Optional Artifact Saved : {output_file}")

    delete_count = sum(1 for r in final_results if r.get("final_action") == "DELETE")
    review_count = sum(1 for r in final_results if r.get("final_action") == "REVIEW")
    keep_count = sum(1 for r in final_results if r.get("final_action") == "KEEP")

    logger.info("📊 [SCAN COMPLETE]")
    logger.info(f"   • Total Processed      : {len(final_results)} emails")
    logger.info(f"   • Flagged for DELETE   : {delete_count}")
    logger.info(f"   • Flagged for REVIEW   : {review_count}")
    logger.info(f"   • Flagged to KEEP      : {keep_count}")
    logger.info("   • Synchronized to DB   : SQLite emails.db (100% DB-driven)")
    return output_file or "DB_UPDATED"


def main():
    parser = argparse.ArgumentParser(description="Step 2: Classify emails with Gemini AI")
    parser.add_argument("--input", type=str, default=None, help="Input fetch CSV path (default: latest in 1_fetch/)")
    parser.add_argument("--output", type=str, default=None, help="Output scan CSV path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size per prompt")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, help="Concurrent workers")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    parser.add_argument("--only-kept", action="store_true", help="Only scan rows previously marked as KEEP")
    args = parser.parse_args()

    run_scan(
        input_file=args.input,
        output_file=args.output,
        batch_size=args.batch_size,
        workers=args.workers,
        email_addr=args.email,
        only_kept=args.only_kept
    )


if __name__ == "__main__":
    main()
