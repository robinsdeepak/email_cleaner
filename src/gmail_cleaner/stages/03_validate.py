"""Step 3: Validate candidate deletions with LLM Safety Auditor (0 IMAP connections)."""

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
    audit_batch_with_gemini,
)
from gmail_cleaner.state import (
    get_latest_artifact,
    generate_artifact_path,
)


def run_validate(input_file=None, output_file=None, batch_size=DEFAULT_BATCH_SIZE,
                 workers=DEFAULT_MAX_WORKERS, email_addr=None):
    """
    Step 3: Reads candidate DELETE emails from 2_scan/, runs secondary LLM Safety Auditor
    (Zero IMAP connections) to rescue receipts, tickets, and sensitive personal emails.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("2_scan", target_account)

    print("\n" + "=" * 65)
    print(f"🛡️ [STEP 3: SAFETY VALIDATION AUDIT (GEMINI AI)]")
    print(f"   • Account      : {target_account}")
    print(f"   • Input File   : {input_file}")
    print(f"   • Concurrency  : {workers} workers")
    print("=" * 65)

    if not input_file or not os.path.exists(input_file):
        print(f"❌ Error: Input artifact '{input_file}' not found. Please run Step 2 (scan) first.")
        return None

    rows = []
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)

    candidates_to_audit = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    print(f"Loaded {len(rows)} emails. Candidate DELETES to audit: {len(candidates_to_audit)}")

    audit_map = {}
    rescued_count = 0
    confirmed_delete_count = 0

    if candidates_to_audit:
        ai_client = get_genai_client()
        chunks = [candidates_to_audit[i:i + batch_size] for i in range(0, len(candidates_to_audit), batch_size)]
        start_time = time.time()

        def process_audit_chunk(chunk):
            payload = [
                {
                    "id": item["uid"],
                    "from": item.get("from", ""),
                    "subject": item.get("subject", ""),
                    "snippet": item.get("snippet", ""),
                    "ai_reason": item.get("ai_reason", "")
                }
                for item in chunk
            ]
            return audit_batch_with_gemini(ai_client, payload)

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(process_audit_chunk, c) for c in chunks]
            for f in concurrent.futures.as_completed(futures):
                try:
                    res = f.result()
                    for item in res:
                        if item.get("id"):
                            audit_map[item["id"]] = item
                except Exception as exc:
                    print(f"⚠️ Audit batch exception: {exc}")

        elapsed = time.time() - start_time
        print(f"Audited {len(candidates_to_audit)} candidates in {elapsed:.2f}s.")

    for r in rows:
        uid = r["uid"]
        if uid in audit_map:
            audit_item = audit_map[uid]
            v_decision = audit_item.get("validator_decision", "CONFIRMED_DELETE")
            v_reason = audit_item.get("validator_reason", "Confirmed by auditor")

            r["validator_decision"] = v_decision
            r["validator_reason"] = v_reason

            if v_decision == "OVERRIDE_KEEP":
                rescued_count += 1
                r["final_action"] = "KEEP"
                print(f"   🚨 [RESCUED FALSE POSITIVE] UID {uid} | '{r.get('subject', '')[:40]}' | Reason: {v_reason}")
            else:
                confirmed_delete_count += 1
        elif (r.get("final_action") or "").strip().upper() != "DELETE":
            r["validator_decision"] = "N/A"
            r["validator_reason"] = r.get("ai_reason", "Retained by initial scan")

    if output_file is None:
        output_file = generate_artifact_path("3_validate", "validated", target_account)

    fieldnames = [
        "uid", "message_id", "date", "from", "subject", "snippet",
        "is_starred", "is_reply", "ai_decision", "ai_reason",
        "validator_decision", "validator_reason", "final_action"
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n📊 [VALIDATION COMPLETE]")
    print(f"   • Confirmed safe to delete: {confirmed_delete_count}")
    print(f"   • Rescued false positives : {rescued_count} (switched to KEEP)")
    print(f"📁 Output Artifact Saved  : {output_file}\n")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Step 3: Validate deletion candidates with LLM Safety Auditor")
    parser.add_argument("--input", type=str, default=None, help="Input scan CSV path (default: latest in 2_scan/)")
    parser.add_argument("--output", type=str, default=None, help="Output validated CSV path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size per prompt")
    parser.add_argument("--workers", type=int, default=DEFAULT_MAX_WORKERS, help="Concurrent workers")
    parser.add_argument("--email", type=str, default=None, help="Target email account")
    args = parser.parse_args()

    run_validate(
        input_file=args.input,
        output_file=args.output,
        batch_size=args.batch_size,
        workers=args.workers,
        email_addr=args.email
    )


if __name__ == "__main__":
    main()
