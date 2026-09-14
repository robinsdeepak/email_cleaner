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
from gmail_cleaner.logger import get_logger, setup_logger

logger = get_logger("streaming")
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
    setup_logger(email_addr=target_account)

    logger.info("=" * 70)
    logger.info("⚡ [STARTING STREAMING PIPELINE (OVERLAPPED I/O & LLM COMPUTE)]")
    logger.info(f"   • Account          : {target_account}")
    logger.info(f"   • Target Limit     : {limit} emails")
    logger.info(f"   • IMAP Connections : {fetch_conns} parallel sockets")
    logger.info(f"   • LLM Concurrency  : {workers} workers ({tier.upper()} tier pacing)")
    logger.info(f"   • Batch Size       : {batch_size} emails/request")
    logger.info("=" * 70)

    # 1. State and Cursor Setup
    state = load_state(target_account)
    if reset_cursor:
        logger.info("🔄 Resetting cursor to start from the beginning.")
        state["last_processed_uid"] = 0

    # 2. Search candidate UIDs
    logger.info("Querying mailbox for candidate UIDs...")
    probe_mail = connect_imap(email_user=target_account)
    probe_mail.select("INBOX")

    last_uid = state.get("last_processed_uid", 0)
    if direction == "oldest-first" and last_uid > 0:
        status, messages = probe_mail.uid("search", None, f"UID {last_uid + 1}:*")
    else:
        status, messages = probe_mail.uid("search", None, "ALL")

    if status != "OK" or not messages[0]:
        logger.info("✅ No emails found matching search criteria.")
        probe_mail.close()
        probe_mail.logout()
        return None

    raw_uids = [int(x) for x in messages[0].split()]
    if direction == "oldest-first" and last_uid > 0:
        raw_uids = [u for u in raw_uids if u > last_uid]

    if not raw_uids:
        logger.info("✅ No new emails to process since last cursor.")
        probe_mail.close()
        probe_mail.logout()
        return None

    if direction == "oldest-first":
        raw_uids.sort()
    else:
        raw_uids.sort(reverse=True)

    selected_uids = raw_uids[:limit]
    logger.info(f"Found {len(raw_uids)} candidates. Streaming next {len(selected_uids)} emails...")
    logger.debug(f"Selected UID range: {selected_uids[0]}..{selected_uids[-1]}")
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
            logger.debug(f"[Stage 1: Fetch] Producer started with {effective_conns} IMAP connections for {len(chunks)} chunks.")

            if effective_conns <= 1:
                # Single connection fetch
                f_conn = connect_imap(email_user=target_account)
                f_conn.select("INBOX")
                for c in chunks:
                    if stop_event.is_set():
                        break
                    batch_rows = fetch_batch_uids_fast(f_conn, c, snippet_length=snippet_length)
                    fetch_queue.put(batch_rows)
                    logger.debug(f"[Stage 1: Fetch] Enqueued {len(batch_rows)} rows (fetch_queue qsize={fetch_queue.qsize()})")
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
                        logger.debug(f"[Stage 1: Fetch] Slice enqueued {len(b_rows)} rows (fetch_queue qsize={fetch_queue.qsize()})")
                    s_conn.close()
                    s_conn.logout()

                # Distribute chunk batches round-robin across connections
                conn_slices = [[] for _ in range(effective_conns)]
                for idx, c in enumerate(chunks):
                    conn_slices[idx % effective_conns].append(c)

                with concurrent.futures.ThreadPoolExecutor(max_workers=effective_conns) as executor:
                    futures = [executor.submit(fetch_slice, s) for s in conn_slices if s]
                    concurrent.futures.wait(futures)

            logger.debug("[Stage 1: Fetch] Producer completed all chunks.")
        except Exception as e:
            logger.error(f"[Stage 1: Fetch] Producer error: {e}", exc_info=True)
        finally:
            fetch_queue.put(_SENTINEL)
            logger.debug("[Stage 1: Fetch] Sentinel placed on fetch_queue.")

    # ------------------ STAGE 2: SCAN WORKER ------------------
    def scan_worker():
        try:
            logger.debug("[Stage 2: Scan] Worker thread active.")
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

                auto_prot = len(batch_rows) - len(ai_candidates)
                logger.debug(f"[Stage 2: Scan] Dequeued {len(batch_rows)} rows. Auto-protected: {auto_prot}, Candidates for LLM: {len(ai_candidates)}")

                if ai_candidates:
                    payload = [
                        {
                            "id": r["uid"],
                            "date": r.get("date", ""),
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
                logger.debug(f"[Stage 2: Scan] Enqueued {len(final_batch)} classified rows to scan_queue (qsize={scan_queue.qsize()})")
        except Exception as e:
            logger.error(f"[Stage 2: Scan] Worker error: {e}", exc_info=True)
        finally:
            scan_queue.put(_SENTINEL)
            logger.debug("[Stage 2: Scan] Sentinel placed on scan_queue.")

    # ------------------ STAGE 3: AUDIT WORKER ------------------
    def audit_worker():
        try:
            logger.debug("[Stage 3: Audit] Worker thread active.")
            while not stop_event.is_set():
                item = scan_queue.get()
                if item is _SENTINEL:
                    scan_queue.task_done()
                    break

                batch_rows = item
                to_audit = [r for r in batch_rows if (r.get("final_action") or "").strip().upper() == "DELETE"]

                if to_audit:
                    logger.debug(f"[Stage 3: Audit] Auditing {len(to_audit)} candidate deletes from batch of {len(batch_rows)} rows...")
                    payload = [
                        {
                            "id": r["uid"],
                            "date": r.get("date", ""),
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

                    rescued_in_batch = 0
                    for r in batch_rows:
                        uid = r["uid"]
                        if uid in audit_map:
                            audit_data = audit_map[uid]
                            v_dec = audit_data.get("validator_decision", "CONFIRMED_DELETE")
                            v_rsn = audit_data.get("validator_reason", "Confirmed by auditor")
                            r["validator_decision"] = v_dec
                            r["validator_reason"] = v_rsn

                            if v_dec == "OVERRIDE_KEEP":
                                rescued_in_batch += 1
                                r["final_action"] = "KEEP"
                                logger.info(f"   🚨 [RESCUED FALSE POSITIVE] UID {uid} | '{r.get('subject', '')[:40]}' | Reason: {v_rsn}")
                        elif (r.get("final_action") or "").strip().upper() != "DELETE":
                            r["validator_decision"] = "N/A"
                            r["validator_reason"] = r.get("ai_reason", "Retained")
                    logger.debug(f"[Stage 3: Audit] Batch audited: {rescued_in_batch} rescued, {len(to_audit) - rescued_in_batch} confirmed delete.")
                else:
                    for r in batch_rows:
                        r["validator_decision"] = "N/A"
                        r["validator_reason"] = r.get("ai_reason", "Retained")

                audit_queue.put(batch_rows)
                scan_queue.task_done()
                logger.debug(f"[Stage 3: Audit] Enqueued {len(batch_rows)} audited rows to audit_queue (qsize={audit_queue.qsize()})")
        except Exception as e:
            logger.error(f"[Stage 3: Audit] Worker error: {e}", exc_info=True)
        finally:
            audit_queue.put(_SENTINEL)
            logger.debug("[Stage 3: Audit] Sentinel placed on audit_queue.")

    # Launch background stage threads
    t_fetch = threading.Thread(target=fetch_producer, name="Stage1-Fetch", daemon=True)
    t_scan = threading.Thread(target=scan_worker, name="Stage2-Scan", daemon=True)
    t_audit = threading.Thread(target=audit_worker, name="Stage3-Audit", daemon=True)

    t_fetch.start()
    t_scan.start()
    t_audit.start()

    # ------------------ STAGE 4: LIVE DISK WRITER ------------------
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
                        total_confirmed_delete += 1
                        r["revalidation_status"] = "CONFIRMED_DELETE"
                        r["revalidation_notes"] = r.get("validator_reason", "Confirmed by auditor")
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
                logger.info(f"   [Stream Progress] Processed: {total_processed}/{len(selected_uids)} | "
                            f"Delete: {total_confirmed_delete} | Keep: {total_processed - total_confirmed_delete} "
                            f"({elapsed_now:.1f}s)")

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrupted by user! Saving all processed batches...")
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

    logger.info("=" * 70)
    logger.info(f"🎉 [STREAMING PIPELINE COMPLETE] in {elapsed_total:.2f}s ({total_processed / max(elapsed_total, 0.01):.1f} emails/s)")
    logger.info(f"   • Total Processed    : {total_processed} emails")
    logger.info(f"   • Confirmed to DELETE: {total_confirmed_delete}")
    logger.info(f"   • Confirmed to KEEP  : {total_processed - total_confirmed_delete}")
    logger.info(f"   • Cursor Updated to  : UID {state['last_processed_uid']}")
    logger.info(f"📁 Reviewed Artifact   : {output_file}")
    logger.info("=" * 70)

    if auto_delete:
        logger.info("Proceeding with live deletion as requested (--auto-delete)...")
        run_delete(input_file=output_file, dry_run=False, email_addr=target_account)
    else:
        logger.info("👉 To preview deletions: make dry-run (or: python pipeline.py dry-run)")
        logger.info("👉 To permanently move confirmed emails to Gmail Trash: make delete (or: python pipeline.py delete)")

    return output_file
