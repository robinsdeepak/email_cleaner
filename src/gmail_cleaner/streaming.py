"""
High-throughput streaming pipeline engine using bounded Producer-Consumer queues.
Overlaps IMAP network fetching, Gemini AI scanning, Safety Auditing, and incremental disk writing.
"""

import concurrent.futures
import csv
from datetime import datetime
import os
import queue
import re
import signal
import sys
import threading
import time

from gmail_cleaner.config import (
    GMAIL_USER,
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_WORKERS,
    DEFAULT_SNIPPET_LENGTH,
)
from gmail_cleaner.imap_client import (
    connect_imap,
    fetch_batch_uids_fast,
)
from gmail_cleaner.ai import (
    get_genai_client,
    classify_batch_with_gemini,
    audit_batch_with_gemini,
)
from gmail_cleaner.limiter import get_rate_limiter
from gmail_cleaner.state import (
    load_state,
    save_state,
    generate_artifact_path,
)
from gmail_cleaner.stages import run_delete

# Regex safety scanner
HIGH_RISK_KEYWORDS = [
    r"\breceipt\b", r"\binvoice\b", r"\border\b", r"\bbooking\b",
    r"\bflight\b", r"\bticket\b", r"\bhotel\b", r"\bstatement\b",
    r"\bbank\b", r"\bpassword\b", r"\b2fa\b", r"\botp\b",
    r"\bverification\b", r"\btax\b", r"\bsalary\b", r"\bpayroll\b"
]
KEYWORD_PATTERN = re.compile("|".join(HIGH_RISK_KEYWORDS), re.IGNORECASE)

_SENTINEL = object()


def run_streaming_pipeline(limit=100, direction="oldest-first", workers=DEFAULT_MAX_WORKERS,
                           batch_size=DEFAULT_BATCH_SIZE, snippet_length=DEFAULT_SNIPPET_LENGTH,
                           reset_cursor=False, auto_delete=False, email_addr=None,
                           fetch_conns=3, tier="paid"):
    """
    Executes the streaming pipeline end-to-end with bounded queues and backpressure:
    Stage 1 (IMAP Pool) -> Stage 2 (Gemini Classifier) -> Stage 3 (Safety Auditor) -> Stage 4 (Live CSV Flush).
    """
    target_account = email_addr or GMAIL_USER
    print("\n" + "=" * 70)
    print("⚡ [STARTING STREAMING PIPELINE (OVERLAPPED I/O & LLM COMPUTE)]")
    print(f"   • Account          : {target_account}")
    print(f"   • Target Limit     : {limit} emails")
    print(f"   • IMAP Connections : {fetch_conns} parallel sockets")
    print(f"   • LLM Concurrency  : {workers} workers ({tier.upper()} tier pacing)")
    print(f"   • Batch Size       : {batch_size} emails/request")
    print("=" * 70)

    # 1. State and Cursor Setup
    state = load_state(target_account)
    if reset_cursor:
        print("🔄 Resetting cursor to start from the beginning.")
        state["last_processed_uid"] = 0

    # 2. Search candidate UIDs
    print("Querying mailbox for candidate UIDs...")
    probe_mail = connect_imap(email_user=target_account)
    probe_mail.select("INBOX")

    last_uid = state.get("last_processed_uid", 0)
    if direction == "oldest-first" and last_uid > 0:
        status, messages = probe_mail.uid("search", None, f"UID {last_uid + 1}:*")
    else:
        status, messages = probe_mail.uid("search", None, "ALL")

    if status != "OK" or not messages[0]:
        print("✅ No emails found matching search criteria.")
        probe_mail.close()
        probe_mail.logout()
        return None

    raw_uids = [int(x) for x in messages[0].split()]
    if direction == "oldest-first" and last_uid > 0:
        raw_uids = [u for u in raw_uids if u > last_uid]

    if not raw_uids:
        print("✅ No new emails to process since last cursor.")
        probe_mail.close()
        probe_mail.logout()
        return None

    if direction == "oldest-first":
        raw_uids.sort()
    else:
        raw_uids.sort(reverse=True)

    selected_uids = raw_uids[:limit]
    print(f"Found {len(raw_uids)} candidates. Streaming next {len(selected_uids)} emails...\n")
    probe_mail.close()
    probe_mail.logout()

    # 3. Queues with bounded backpressure
    fetch_queue = queue.Queue(maxsize=3)
    scan_queue = queue.Queue(maxsize=3)
    audit_queue = queue.Queue(maxsize=3)

    rate_limiter = get_rate_limiter(tier=tier)
    ai_client = get_genai_client()
    stop_event = threading.Event()
    start_time = time.time()

    # Prepare review CSV file
    output_file = generate_artifact_path("4_revalidate", "revalidated", target_account)
    fieldnames = [
        "uid", "message_id", "date", "from", "subject", "snippet",
        "is_starred", "is_reply", "ai_decision", "ai_reason",
        "validator_decision", "validator_reason",
        "revalidation_status", "revalidation_notes",
        "final_action"
    ]

    total_processed = 0
    total_confirmed_delete = 0
    total_rescued = 0
    max_observed_uid = last_uid

    # ------------------ STAGE 1: FETCH PRODUCER ------------------
    def fetch_producer():
        try:
            chunks = [selected_uids[i:i + batch_size] for i in range(0, len(selected_uids), batch_size)]
            effective_conns = min(fetch_conns, len(chunks))

            if effective_conns <= 1:
                # Single connection fetch
                f_conn = connect_imap(email_user=target_account)
                f_conn.select("INBOX")
                for c in chunks:
                    if stop_event.is_set():
                        break
                    batch_rows = fetch_batch_uids_fast(f_conn, c, snippet_length=snippet_length)
                    fetch_queue.put(batch_rows)
                f_conn.close()
                f_conn.logout()
            else:
                # Multi-connection partitioned fetch
                def fetch_slice(slice_chunks):
                    s_conn = connect_imap(email_user=target_account)
                    s_conn.select("INBOX")
                    for c in slice_chunks:
                        if stop_event.is_set():
                            break
                        b_rows = fetch_batch_uids_fast(s_conn, c, snippet_length=snippet_length)
                        fetch_queue.put(b_rows)
                    s_conn.close()
                    s_conn.logout()

                # Distribute chunk batches round-robin across connections
                conn_slices = [[] for _ in range(effective_conns)]
                for idx, c in enumerate(chunks):
                    conn_slices[idx % effective_conns].append(c)

                with concurrent.futures.ThreadPoolExecutor(max_workers=effective_conns) as executor:
                    futures = [executor.submit(fetch_slice, s) for s in conn_slices if s]
                    concurrent.futures.wait(futures)

        except Exception as e:
            print(f"⚠️ Fetch producer error: {e}")
        finally:
            fetch_queue.put(_SENTINEL)

    # ------------------ STAGE 2: SCAN WORKER ------------------
    def scan_worker():
        try:
            while not stop_event.is_set():
                item = fetch_queue.get()
                if item is _SENTINEL:
                    fetch_queue.task_done()
                    break

                batch_rows = item
                ai_candidates = []
                final_batch = []

                for r in batch_rows:
                    is_starred = r.get("is_starred", "FALSE").upper() == "TRUE"
                    is_reply = r.get("is_reply", "FALSE").upper() == "TRUE"

                    if is_starred:
                        r["ai_decision"] = "KEEP"
                        r["ai_reason"] = "Auto-Protected: Starred Email"
                        r["final_action"] = "KEEP"
                        final_batch.append(r)
                    elif is_reply:
                        r["ai_decision"] = "KEEP"
                        r["ai_reason"] = "Auto-Protected: Thread Reply / Reference"
                        r["final_action"] = "KEEP"
                        final_batch.append(r)
                    else:
                        ai_candidates.append(r)

                if ai_candidates:
                    payload = [
                        {
                            "id": r["uid"],
                            "from": r.get("from", ""),
                            "subject": r.get("subject", ""),
                            "snippet": r.get("snippet", "")
                        }
                        for r in ai_candidates
                    ]
                    rate_limiter.acquire(1)
                    eval_results = classify_batch_with_gemini(ai_client, payload)
                    eval_map = {str(res.get("id")): res for res in eval_results if res.get("id")}

                    for r in ai_candidates:
                        uid = r["uid"]
                        eval_data = eval_map.get(uid, {})
                        should_del = eval_data.get("delete", False)
                        decision = "DELETE" if should_del else "KEEP"
                        r["ai_decision"] = decision
                        r["ai_reason"] = eval_data.get("reason", "Unclassified")
                        r["final_action"] = decision
                        final_batch.append(r)

                # Preserve ordering
                uid_pos = {r["uid"]: idx for idx, r in enumerate(batch_rows)}
                final_batch.sort(key=lambda x: uid_pos.get(x["uid"], 0))

                scan_queue.put(final_batch)
                fetch_queue.task_done()
        except Exception as e:
            print(f"⚠️ Scan worker error: {e}")
        finally:
            scan_queue.put(_SENTINEL)

    # ------------------ STAGE 3: AUDIT WORKER ------------------
    def audit_worker():
        try:
            while not stop_event.is_set():
                item = scan_queue.get()
                if item is _SENTINEL:
                    scan_queue.task_done()
                    break

                batch_rows = item
                to_audit = [r for r in batch_rows if (r.get("final_action") or "").strip().upper() == "DELETE"]

                if to_audit:
                    payload = [
                        {
                            "id": r["uid"],
                            "from": r.get("from", ""),
                            "subject": r.get("subject", ""),
                            "snippet": r.get("snippet", ""),
                            "ai_reason": r.get("ai_reason", "")
                        }
                        for r in to_audit
                    ]
                    rate_limiter.acquire(1)
                    audit_results = audit_batch_with_gemini(ai_client, payload)
                    audit_map = {str(res.get("id")): res for res in audit_results if res.get("id")}

                    for r in batch_rows:
                        uid = r["uid"]
                        if uid in audit_map:
                            audit_data = audit_map[uid]
                            v_dec = audit_data.get("validator_decision", "CONFIRMED_DELETE")
                            v_rsn = audit_data.get("validator_reason", "Confirmed by auditor")
                            r["validator_decision"] = v_dec
                            r["validator_reason"] = v_rsn

                            if v_dec == "OVERRIDE_KEEP":
                                r["final_action"] = "KEEP"
                        elif (r.get("final_action") or "").strip().upper() != "DELETE":
                            r["validator_decision"] = "N/A"
                            r["validator_reason"] = r.get("ai_reason", "Retained")
                else:
                    for r in batch_rows:
                        r["validator_decision"] = "N/A"
                        r["validator_reason"] = r.get("ai_reason", "Retained")

                audit_queue.put(batch_rows)
                scan_queue.task_done()
        except Exception as e:
            print(f"⚠️ Audit worker error: {e}")
        finally:
            audit_queue.put(_SENTINEL)

    # Launch background stage threads
    t_fetch = threading.Thread(target=fetch_producer, name="Stage1-Fetch", daemon=True)
    t_scan = threading.Thread(target=scan_worker, name="Stage2-Scan", daemon=True)
    t_audit = threading.Thread(target=audit_worker, name="Stage3-Audit", daemon=True)

    t_fetch.start()
    t_scan.start()
    t_audit.start()

    # ------------------ STAGE 4: HEURISTIC & LIVE DISK WRITER ------------------
    try:
        with open(output_file, "w", newline="", encoding="utf-8") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
            writer.writeheader()
            out_f.flush()

            while not stop_event.is_set():
                item = audit_queue.get()
                if item is _SENTINEL:
                    audit_queue.task_done()
                    break

                batch_rows = item
                for r in batch_rows:
                    total_processed += 1
                    current_act = (r.get("final_action") or "").strip().upper()

                    if current_act == "DELETE":
                        subject = r.get("subject", "")
                        snippet = r.get("snippet", "")
                        full_txt = f"{subject} {snippet}"

                        m = KEYWORD_PATTERN.search(full_txt)
                        if m:
                            total_rescued += 1
                            kw = m.group(0).lower()
                            r["revalidation_status"] = "FLAGGED_HIGH_RISK"
                            r["revalidation_notes"] = f"Rescued: Contains high-risk keyword '{kw}'"
                            r["final_action"] = "KEEP"
                        else:
                            total_confirmed_delete += 1
                            r["revalidation_status"] = "VERIFIED_SAFE"
                            r["revalidation_notes"] = "Passed all safety checks"
                    else:
                        r["revalidation_status"] = "KEPT"
                        r["revalidation_notes"] = r.get("validator_reason", "Retained")

                    try:
                        u_int = int(r["uid"])
                        if u_int > max_observed_uid:
                            max_observed_uid = u_int
                    except Exception:
                        pass

                writer.writerows(batch_rows)
                out_f.flush()  # Incremental flush ensures zero lost progress!
                audit_queue.task_done()

                elapsed_now = time.time() - start_time
                print(f"   [Stream Progress] Processed: {total_processed}/{len(selected_uids)} | "
                      f"Delete: {total_confirmed_delete} | Keep: {total_processed - total_confirmed_delete} "
                      f"({elapsed_now:.1f}s)")

    except KeyboardInterrupt:
        print("\n\n⚠️ Interrupted by user! Saving all processed batches...")
        stop_event.set()

    t_fetch.join(timeout=3)
    t_scan.join(timeout=3)
    t_audit.join(timeout=3)

    elapsed_total = time.time() - start_time

    # Update state cursor
    if direction == "oldest-first" and max_observed_uid > last_uid:
        state["last_processed_uid"] = max_observed_uid
    state["total_scanned"] = state.get("total_scanned", 0) + total_processed
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state, target_account)

    print("\n" + "=" * 70)
    print(f"🎉 [STREAMING PIPELINE COMPLETE] in {elapsed_total:.2f}s ({total_processed / max(elapsed_total, 0.01):.1f} emails/s)")
    print(f"   • Total Processed    : {total_processed} emails")
    print(f"   • Confirmed to DELETE: {total_confirmed_delete}")
    print(f"   • Confirmed to KEEP  : {total_processed - total_confirmed_delete}")
    print(f"   • Cursor Updated to  : UID {state['last_processed_uid']}")
    print(f"📁 Reviewed Artifact   : {output_file}")
    print("=" * 70)

    if auto_delete:
        print("\nProceeding with live deletion as requested (--auto-delete)...")
        run_delete(input_file=output_file, dry_run=False, email_addr=target_account)
    else:
        print("\n👉 To preview deletions: make dry-run (or: python pipeline.py dry-run)")
        print("👉 To permanently move confirmed emails to Gmail Trash: make delete (or: python pipeline.py delete)\n")

    return output_file
