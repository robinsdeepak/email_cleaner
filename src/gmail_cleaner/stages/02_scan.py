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
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)


def run_scan(input_file=None, output_file=None, batch_size=DEFAULT_BATCH_SIZE,
             workers=DEFAULT_MAX_WORKERS, email_addr=None, only_kept=False):
    """
    Step 2: Reads fetched artifact from 1_fetch/, runs parallel Gemini classification
    (Zero IMAP connections), and writes outputs/<email>/2_scan/scanned_<timestamp>.csv.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("1_fetch", target_account)

    print("\n" + "=" * 65)
    print(f"🤖 [STEP 2: SCAN & CLASSIFY (GEMINI AI)]")
    print(f"   • Account      : {target_account}")
    print(f"   • Input File   : {input_file}")
    print(f"   • Concurrency  : {workers} workers")
    print(f"   • Batch Size   : {batch_size} emails/request")
    if only_kept:
        print(f"   • Filter       : Only scanning emails previously marked as KEEP")
    print("=" * 65)

    if not input_file or not os.path.exists(input_file):
        print(f"❌ Error: Input artifact '{input_file}' not found. Please run Step 1 (fetch) first.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if only_kept and (r.get("final_action") or "").strip().upper() == "DELETE":
                continue
            rows.append(r)

    if not rows:
        print("⚠️ Input file is empty (or no KEEP candidates remained).")
        return None

    print(f"Loaded {len(rows)} emails to classify.")

    # Pre-filter Starred and Reply emails
    ai_candidates = []
    final_results = []
    skipped_count = 0

    for r in rows:
        is_starred = r.get("is_starred", "FALSE").upper() == "TRUE"
        is_reply = r.get("is_reply", "FALSE").upper() == "TRUE"

        if is_starred:
            skipped_count += 1
            r["ai_decision"] = "KEEP"
            r["ai_reason"] = "Auto-Protected: Starred Email"
            r["final_action"] = "KEEP"
            final_results.append(r)
        elif is_reply:
            skipped_count += 1
            r["ai_decision"] = "KEEP"
            r["ai_reason"] = "Auto-Protected: Thread Reply / Reference"
            r["final_action"] = "KEEP"
            final_results.append(r)
        else:
            ai_candidates.append(r)

    print(f"Auto-protected {skipped_count} Starred/Reply emails (0 tokens used).")
    print(f"Sending {len(ai_candidates)} emails to Gemini for classification...")

    if ai_candidates:
        ai_client = get_genai_client()
        chunks = [ai_candidates[i:i + batch_size] for i in range(0, len(ai_candidates), batch_size)]
        start_time = time.time()
        eval_map = {}

        def process_chunk(chunk):
            payload = [
                {
                    "id": item["uid"],
                    "date": item.get("date", ""),
                    "from": item.get("from", ""),
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
                    print(f"⚠️ Batch generated an exception: {exc}")
                print(f"   Progress: [{completed_chunks}/{len(chunks)}] batches classified...")

        for item in ai_candidates:
            uid = item["uid"]
            evaluated = eval_map.get(uid, {})
            should_delete = evaluated.get("delete", False)
            decision = "DELETE" if should_delete else "KEEP"
            reason = evaluated.get("reason", "Unclassified")

            item["ai_decision"] = decision
            item["ai_reason"] = reason
            item["final_action"] = decision
            final_results.append(item)

        elapsed = time.time() - start_time
        print(f"Classified {len(ai_candidates)} emails in {elapsed:.2f}s.")

    # Sort to preserve original UID order
    uid_order = {r["uid"]: idx for idx, r in enumerate(rows)}
    final_results.sort(key=lambda x: uid_order.get(x["uid"], 0))

    if output_file is None:
        output_file = generate_artifact_path("2_scan", "scanned", target_account)

    fieldnames = ["uid", "message_id", "date", "from", "subject", "snippet", "is_starred", "is_reply", "ai_decision", "ai_reason", "final_action"]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(final_results)

    delete_count = sum(1 for r in final_results if r.get("ai_decision") == "DELETE")
    keep_count = len(final_results) - delete_count

    print(f"\n📊 [SCAN COMPLETE]")
    print(f"   • Total Processed      : {len(final_results)} emails")
    print(f"   • Recommended DELETE   : {delete_count}")
    print(f"   • Recommended KEEP     : {keep_count}")
    print(f"📁 Output Artifact Saved  : {output_file}\n")
    return output_file


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
