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
from gmail_cleaner.db import EmailDB
from gmail_cleaner.worker import worker

_SENTINEL = object()


def run_streaming_pipeline(limit=100, direction="oldest-first", workers=DEFAULT_MAX_WORKERS,
                           batch_size=DEFAULT_BATCH_SIZE, snippet_length=DEFAULT_SNIPPET_LENGTH,
                           reset_cursor=False, auto_delete=False, email_addr=None,
                           fetch_conns=3, tier="paid", run_id=None):
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
    if reset_cursor:
        logger.info("🔄 Resetting cursor to start from the beginning.")
        db.update_account_cursor(target_account, 0)

    last_uid = 0 if reset_cursor else db.get_account_cursor(target_account)

    # 2. Search candidate UIDs
    logger.info("Querying mailbox for candidate UIDs...")
    probe_mail = connect_imap(email_user=target_account)
    probe_mail.select("INBOX")

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
    db = EmailDB(account=target_account)

    # Ensure run_id is ALWAYS present and recorded
    if not run_id:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{timestamp}_stream"
        db.create_run(
            action_type="Streaming Pipeline",
            run_id=run_id,
            params={"limit": limit, "direction": direction, "workers": workers, "batch_size": batch_size, "tier": tier},
        )

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
    total_needs_review = 0
    total_kept = 0
    max_observed_uid = last_uid

    # ------------------ STAGE 1: FETCH PRODUCER ------------------
    def fetch_producer():
        try:
            fetch_chunk_size = max(50, batch_size)
            chunks = [selected_uids[i:i + fetch_chunk_size] for i in range(0, len(selected_uids), fetch_chunk_size)]
            effective_conns = min(fetch_conns, len(chunks))
            logger.debug(f"[Stage 1: Fetch] Producer started with {effective_conns} IMAP connections for {len(chunks)} chunks (chunk size {fetch_chunk_size}).")

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
            logger.debug(f"[Stage 2: Scan] Worker thread active (batch_size={batch_size}, concurrency={workers}).")
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
                        r["ai_decision"] = "CONFIDENT_KEEP"
                        r["ai_confidence"] = "HIGH"
                        r["ai_category"] = "PERSONAL"
                        r["ai_reason"] = "Auto-Protected: Starred Email"
                        r["final_action"] = "KEEP"
                        final_batch.append(r)
                    elif is_reply:
                        r["ai_decision"] = "CONFIDENT_KEEP"
                        r["ai_confidence"] = "HIGH"
                        r["ai_category"] = "PERSONAL"
                        r["ai_reason"] = "Auto-Protected: Thread Reply / Reference"
                        r["final_action"] = "KEEP"
                        final_batch.append(r)
                    else:
                        ai_candidates.append(r)

                auto_prot = len(batch_rows) - len(ai_candidates)
                logger.debug(f"[Stage 2: Scan] Dequeued {len(batch_rows)} rows. Auto-protected: {auto_prot}, Candidates for LLM: {len(ai_candidates)}")

                if ai_candidates:
                    eval_map = {}
                    if batch_size <= 1:
                        # Individual 1-email-per-call execution across worker thread pool
                        def eval_one(r):
                            p = [{
                                "id": r["uid"],
                                "date": r.get("date", ""),
                                "from": r.get("from", ""),
                                "subject": r.get("subject", ""),
                                "snippet": r.get("snippet", "")
                            }]
                            rate_limiter.acquire(1)
                            res = classify_batch_with_gemini(ai_client, p)
                            return res[0] if res else None

                        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                            futures = [pool.submit(eval_one, r) for r in ai_candidates]
                            for f in concurrent.futures.as_completed(futures):
                                res = f.result()
                                if res and res.get("id"):
                                    eval_map[str(res.get("id"))] = res
                    else:
                        sub_chunks = [ai_candidates[i:i + batch_size] for i in range(0, len(ai_candidates), batch_size)]
                        for sub in sub_chunks:
                            payload = [
                                {
                                    "id": r["uid"],
                                    "date": r.get("date", ""),
                                    "from": r.get("from", ""),
                                    "subject": r.get("subject", ""),
                                    "snippet": r.get("snippet", "")
                                }
                                for r in sub
                            ]
                            rate_limiter.acquire(1)
                            eval_results = classify_batch_with_gemini(ai_client, payload)
                            for res in eval_results:
                                if res.get("id"):
                                    eval_map[str(res.get("id"))] = res

                    for r in ai_candidates:
                        uid = r["uid"]
                        eval_data = eval_map.get(uid, {})
                        status_val = (eval_data.get("status") or eval_data.get("ai_decision") or "NEEDS_REVIEW").strip().upper()
                        
                        if status_val in ("CONFIDENT_DELETE", "PROBABLE_DELETE"):
                            action = "DELETE"
                        elif status_val in ("CONFIDENT_KEEP", "PROBABLE_KEEP"):
                            action = "KEEP"
                        else:
                            action = "REVIEW"

                        r["ai_decision"] = status_val
                        r["ai_confidence"] = (eval_data.get("confidence") or "MEDIUM").strip().upper()
                        r["ai_category"] = (eval_data.get("category") or "OTHER").strip().upper()
                        r["ai_reason"] = eval_data.get("reason", "Unclassified")
                        r["final_action"] = action
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
            logger.debug(f"[Stage 3: Audit] Worker thread active (batch_size={batch_size}, concurrency={workers}).")
            while not stop_event.is_set():
                item = scan_queue.get()
                if item is _SENTINEL:
                    scan_queue.task_done()
                    break

                batch_rows = item
                # Audit both candidate deletions AND emails marked as REVIEW
                to_audit = [r for r in batch_rows if (r.get("final_action") or "").strip().upper() in ("DELETE", "REVIEW")]

                if to_audit:
                    logger.debug(f"[Stage 3: Audit] Auditing {len(to_audit)} candidates from batch of {len(batch_rows)} rows...")
                    audit_map = {}
                    if batch_size <= 1:
                        # Individual 1-email-per-call audit across worker thread pool
                        def audit_one(r):
                            p = [{
                                "id": r["uid"],
                                "date": r.get("date", ""),
                                "from": r.get("from", ""),
                                "subject": r.get("subject", ""),
                                "snippet": r.get("snippet", ""),
                                "ai_reason": r.get("ai_reason", "")
                            }]
                            rate_limiter.acquire(1)
                            res = audit_batch_with_gemini(ai_client, p)
                            return res[0] if res else None

                        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                            futures = [pool.submit(audit_one, r) for r in to_audit]
                            for f in concurrent.futures.as_completed(futures):
                                res = f.result()
                                if res and res.get("id"):
                                    audit_map[str(res.get("id"))] = res
                    else:
                        sub_chunks = [to_audit[i:i + batch_size] for i in range(0, len(to_audit), batch_size)]
                        for sub in sub_chunks:
                            payload = [
                                {
                                    "id": r["uid"],
                                    "date": r.get("date", ""),
                                    "from": r.get("from", ""),
                                    "subject": r.get("subject", ""),
                                    "snippet": r.get("snippet", ""),
                                    "ai_reason": r.get("ai_reason", "")
                                }
                                for r in sub
                            ]
                            rate_limiter.acquire(1)
                            audit_results = audit_batch_with_gemini(ai_client, payload)
                            for res in audit_results:
                                if res.get("id"):
                                    audit_map[str(res.get("id"))] = res

                    rescued_in_batch = 0
                    for r in batch_rows:
                        uid = r["uid"]
                        if uid in audit_map:
                            audit_data = audit_map[uid]
                            val_status = (audit_data.get("validator_status") or audit_data.get("validator_decision") or "CONFIRMED_DELETE").strip().upper()
                            val_conf = (audit_data.get("validator_confidence") or "HIGH").strip().upper()
                            val_rsn = audit_data.get("validator_reason", "Confirmed by auditor")
                            
                            r["validator_decision"] = val_status
                            r["validator_confidence"] = val_conf
                            r["validator_reason"] = val_rsn

                            if val_status in ("CONFIRMED_KEEP", "SUGGEST_KEEP"):
                                rescued_in_batch += 1
                                r["final_action"] = "KEEP"
                                logger.info(f"   🚨 [RESCUED FALSE POSITIVE] UID {uid} | '{r.get('subject', '')[:40]}' | Reason: {val_rsn}")
                            elif val_status == "NEEDS_USER_REVIEW":
                                r["final_action"] = "REVIEW"
                                logger.info(f"   🟡 [FLAGGED FOR USER REVIEW] UID {uid} | '{r.get('subject', '')[:40]}' | Reason: {val_rsn}")
                            else:
                                r["final_action"] = "DELETE"

                        elif (r.get("final_action") or "").strip().upper() not in ("DELETE", "REVIEW"):
                            r["validator_decision"] = "CONFIRMED_KEEP"
                            r["validator_confidence"] = "HIGH"
                            r["validator_reason"] = r.get("ai_reason", "Retained by initial scan")

                    logger.debug(f"[Stage 3: Audit] Batch audited: {rescued_in_batch} rescued, {len(to_audit) - rescued_in_batch} remaining.")
                else:
                    for r in batch_rows:
                        r["validator_decision"] = "CONFIRMED_KEEP"
                        r["validator_confidence"] = "HIGH"
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

    # ------------------ STAGE 4: DIRECT SQLITE SYNCHRONIZER ------------------
    try:
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
                elif current_act == "REVIEW":
                    total_needs_review += 1
                    r["revalidation_status"] = "NEEDS_REVIEW"
                    r["revalidation_notes"] = r.get("validator_reason", "Requires human review")
                else:
                    total_kept += 1
                    r["revalidation_status"] = "KEPT"
                    r["revalidation_notes"] = r.get("validator_reason", "Retained")

                if run_id:
                    r["last_run_id"] = run_id

                try:
                    u_int = int(r["uid"])
                    if u_int > max_observed_uid:
                        max_observed_uid = u_int
                except Exception:
                    pass

            # Direct SQLite synchronization (Zero CSV dependency)
            try:
                db.upsert_emails(batch_rows)
            except Exception as db_err:
                logger.warning(f"Could not sync batch to SQLite DB: {db_err}")

            audit_queue.task_done()

            elapsed_now = time.time() - start_time
            progress_msg = (
                f"Processed: {total_processed}/{len(selected_uids)} | "
                f"Delete: {total_confirmed_delete} | Review: {total_needs_review} | Keep: {total_kept} "
                f"({elapsed_now:.1f}s)"
            )
            logger.info(f"   [Stream Progress] {progress_msg}")
            worker.update_progress(
                current=total_processed,
                total=len(selected_uids),
                message=progress_msg
            )

            if worker.is_cancel_requested:
                logger.warning("Cancellation requested by background worker. Stopping stream...")
                stop_event.set()

    except KeyboardInterrupt:
        logger.warning("\n⚠️ Interrupted by user! Saving all processed batches...")
        stop_event.set()

    t_fetch.join(timeout=3)
    t_scan.join(timeout=3)
    t_audit.join(timeout=3)

    elapsed_total = time.time() - start_time

    # Update state cursor
    new_cursor = max_observed_uid if (direction == "oldest-first" and max_observed_uid > last_uid) else last_uid
    acc_stat = db.get_stats()
    db.update_account_cursor(target_account, new_cursor, acc_stat.get("total_emails", total_processed))

    logger.info("=" * 70)
    logger.info(f"🎉 [STREAMING PIPELINE COMPLETE] in {elapsed_total:.2f}s ({total_processed / max(elapsed_total, 0.01):.1f} emails/s)")
    logger.info(f"   • Total Processed    : {total_processed} emails")
    logger.info(f"   • Confirmed to DELETE: {total_confirmed_delete}")
    logger.info(f"   • Flagged for REVIEW : {total_needs_review}")
    logger.info(f"   • Confirmed to KEEP  : {total_kept}")
    logger.info(f"   • Cursor Updated to  : UID {new_cursor}")
    logger.info("   • Persistence Layer  : SQLite emails.db (100% database-driven, 0 CSV dependency)")
    logger.info("=" * 70)

    # Record completed run in database
    if run_id:
        db.update_run(
            run_id,
            status="CANCELLED" if stop_event.is_set() else "COMPLETED",
            duration_seconds=round(elapsed_total, 1),
            total_emails=total_processed,
            delete_count=total_confirmed_delete,
            keep_count=total_kept,
            rescued_count=total_needs_review,
            artifact_path=None,
        )

    if auto_delete:
        logger.info("Proceeding with live deletion as requested (--auto-delete)...")
        run_delete(input_file=None, dry_run=False, email_addr=target_account, run_id=run_id)
    else:
        logger.info("👉 Review emails directly in browser UI (Tab 2: Email Explorer & Review).")
        logger.info("👉 To preview deletions: make dry-run (or: python pipeline.py dry-run)")
        logger.info("👉 To permanently move confirmed emails to Gmail Trash: make delete (or: python pipeline.py delete)")

    return f"Processed {total_processed} emails directly into SQLite ({total_confirmed_delete} delete, {total_needs_review} review, {total_kept} keep)"

