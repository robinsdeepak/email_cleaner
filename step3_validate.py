import argparse
import concurrent.futures
import csv
import json
import os
import sys
import time
from google import genai
from google.genai import types
from common import (
    GMAIL_USER,
    GEMINI_API_KEY,
    MODEL_NAME,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    get_latest_artifact,
    generate_artifact_path,
    safe_parse_json_array,
    validate_gemini_credentials,
)


def audit_batch_with_gemini(client, batch_payload, max_retries=3):
    """Audits deletion candidates specifically to catch false positives."""
    prompt = f"""
You are an expert, highly conservative Email Safety Auditor.
A first-pass system proposed to DELETE the following emails.
Your objective is to CATCH FALSE POSITIVES and rescue any critical emails that should NOT be deleted.

AUDIT RULES:
- OVERRIDE_KEEP: Select this if the email contains ANY of the following:
  * Receipts, invoices, purchase confirmations, order tracking, renewal notices.
  * Travel, hotel, train, or flight bookings, event tickets.
  * Official bank statements, account security alerts, password resets, verification codes.
  * Legal/tax compliance notices, employment or salary communications.
  * Personal correspondence.
- CONFIRMED_DELETE: Pure promotional marketing, sales offers, retail discounts, junk newsletters, cold outreach.

FORMAT:
- Keep 'validator_reason' extremely brief (under 10 words).

Emails to audit:
{json.dumps(batch_payload, indent=2)}
"""
    for attempt in range(max_retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    max_output_tokens=8192,
                    response_schema={
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "id": {"type": "STRING"},
                                "validator_decision": {
                                    "type": "STRING",
                                    "enum": ["CONFIRMED_DELETE", "OVERRIDE_KEEP"]
                                },
                                "validator_reason": {"type": "STRING"}
                            },
                            "required": ["id", "validator_decision", "validator_reason"]
                        }
                    }
                )
            )
            parsed = safe_parse_json_array(response.text)
            if parsed:
                return parsed
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                print(f"   ⏳ Auditor throttled (429). Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    print(f"⚠️ Auditor API error after {max_retries} retries: {e}")
                else:
                    print(f"⚠️ Auditor API error: {e}")
                return []
    return []


def run_validate(input_file=None, output_file=None, batch_size=DEFAULT_BATCH_SIZE,
                 workers=DEFAULT_MAX_WORKERS, email_addr=None):
    """
    Step 3: Reads scan artifact from 2_scan/, runs LLM Safety Auditor on candidate DELETES,
    switches false positives to KEEP, and writes outputs/<email>/3_validate/validate_<timestamp>.csv.
    """
    target_account = email_addr or GMAIL_USER

    if not input_file:
        input_file = get_latest_artifact("2_scan", target_account)

    print("\n" + "=" * 65)
    print(f"🔬 [STEP 3: VALIDATE (LLM FALSE-POSITIVE AUDITOR)]")
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

    if not rows:
        print("⚠️ Input file is empty.")
        return None

    candidates_to_audit = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    print(f"Loaded {len(rows)} emails. Candidate DELETES to audit: {len(candidates_to_audit)}")

    audit_map = {}
    rescued_count = 0
    confirmed_delete_count = 0

    if candidates_to_audit:
        validate_gemini_credentials()
        chunks = [candidates_to_audit[i:i + batch_size] for i in range(0, len(candidates_to_audit), batch_size)]
        ai_client = genai.Client(api_key=GEMINI_API_KEY)
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
    parser = argparse.ArgumentParser(description="Step 3: Validate candidate deletions with LLM Safety Auditor")
    parser.add_argument("--input", type=str, default=None, help="Input scan CSV path (default: latest in 2_scan/)")
    parser.add_argument("--output", type=str, default=None, help="Output validate CSV path")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Batch size")
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
