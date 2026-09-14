"""
SQLite database storage and state management for Gmail AI Cleaner.

Provides thread-safe connections, WAL mode concurrency, schema migrations,
and unified CRUD operations across all pipeline stages.
"""

from contextlib import contextmanager
import csv
from datetime import datetime
import glob
import json
import os
import re
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from gmail_cleaner.config import GMAIL_USER
from gmail_cleaner.logger import get_logger
from gmail_cleaner.state import get_account_dir

logger = get_logger("db")


def get_default_db_path(email_addr: Optional[str] = None) -> str:
    """Returns the default SQLite database path for an email account: outputs/<account>/emails.db."""
    return os.path.join(get_account_dir(email_addr), "emails.db")


class EmailDB:
    """
    Thread-safe SQLite database manager for email lifecycle tracking.
    
    Lifecycle statuses:
      - 'FETCHED'   : Initial fetch from Gmail IMAP (raw headers & snippet saved).
      - 'SCANNED'   : First-pass AI triage complete (ai_decision: KEEP/DELETE).
      - 'AUDITED'   : Safety Auditor evaluation complete (validator_decision: OVERRIDE_KEEP/CONFIRMED_DELETE).
      - 'TRASHED'   : Successfully moved to Gmail Trash.
      - 'RESTORED'  : Restored back to Gmail Inbox from Trash.
    """

    def __init__(self, db_path: Optional[str] = None, account: Optional[str] = None):
        self.account = account or GMAIL_USER
        self.db_path = db_path or get_default_db_path(self.account)
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        self.init_db()

    @contextmanager
    def get_connection(self):
        """Context manager providing an optimized, WAL-enabled SQLite connection."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            # WAL mode allows concurrent readers and high-speed write transactions
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_db(self):
        """Initializes tables, constraints, performance indexes, and column migrations."""
        with self.get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS emails (
                    account             TEXT NOT NULL,
                    uid                 INTEGER NOT NULL,
                    message_id          TEXT,
                    date                TEXT,
                    sender              TEXT,
                    subject             TEXT,
                    snippet             TEXT,
                    is_starred          BOOLEAN DEFAULT 0,
                    is_reply            BOOLEAN DEFAULT 0,
                    
                    status              TEXT DEFAULT 'FETCHED',
                    ai_decision         TEXT,
                    ai_reason           TEXT,
                    validator_decision  TEXT,
                    validator_reason    TEXT,
                    revalidation_status TEXT,
                    revalidation_notes  TEXT,
                    final_action        TEXT,
                    last_run_id         TEXT,
                    
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trashed_at          TIMESTAMP,
                    
                    PRIMARY KEY (account, uid)
                );

                CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(account, status);
                CREATE INDEX IF NOT EXISTS idx_emails_final_action ON emails(account, final_action);
                CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(account, message_id);

                CREATE TABLE IF NOT EXISTS runs (
                    run_id              TEXT PRIMARY KEY,
                    account             TEXT NOT NULL,
                    action_type         TEXT NOT NULL,
                    status              TEXT DEFAULT 'RUNNING',
                    started_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at        TIMESTAMP,
                    duration_seconds    REAL DEFAULT 0.0,
                    total_emails        INTEGER DEFAULT 0,
                    delete_count        INTEGER DEFAULT 0,
                    keep_count          INTEGER DEFAULT 0,
                    rescued_count       INTEGER DEFAULT 0,
                    trashed_count       INTEGER DEFAULT 0,
                    artifact_path       TEXT,
                    params_json         TEXT,
                    error_message       TEXT,
                    notes               TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_runs_account ON runs(account, started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(account, status);
            """)

            # Migration: Ensure last_run_id column exists if table was created in older schema
            cursor = conn.execute("PRAGMA table_info(emails);")
            col_names = [col["name"] for col in cursor.fetchall()]
            if "last_run_id" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN last_run_id TEXT;")
                logger.info("Migrated emails table: added 'last_run_id' column")

        # Auto-backfill historical runs from artifact CSVs
        try:
            self.backfill_historical_runs()
        except Exception as e:
            logger.debug(f"Historical run backfill notice: {e}")

        logger.debug(f"SQLite DB initialized with WAL mode at: {self.db_path}")

    # -------------------------------------------------------------------------
    # CREATE / UPSERT
    # -------------------------------------------------------------------------

    def upsert_emails(self, rows: List[Dict[str, Any]]) -> int:
        """
        Batch inserts or updates emails from fetch or CSV imports.
        Preserves existing decisions/statuses on conflict.
        """
        if not rows:
            return 0

        t0 = time.time()
        sql = """
            INSERT INTO emails (
                account, uid, message_id, date, sender, subject, snippet,
                is_starred, is_reply, status, ai_decision, ai_reason,
                validator_decision, validator_reason, revalidation_status,
                revalidation_notes, final_action, last_run_id, updated_at
            ) VALUES (
                :account, :uid, :message_id, :date, :sender, :subject, :snippet,
                :is_starred, :is_reply, :status, :ai_decision, :ai_reason,
                :validator_decision, :validator_reason, :revalidation_status,
                :revalidation_notes, :final_action, :last_run_id, CURRENT_TIMESTAMP
            )
            ON CONFLICT(account, uid) DO UPDATE SET
                message_id = COALESCE(excluded.message_id, emails.message_id),
                date = COALESCE(excluded.date, emails.date),
                sender = COALESCE(excluded.sender, emails.sender),
                subject = COALESCE(excluded.subject, emails.subject),
                snippet = COALESCE(excluded.snippet, emails.snippet),
                is_starred = excluded.is_starred,
                is_reply = excluded.is_reply,
                status = CASE 
                    WHEN emails.status IN ('SCANNED', 'AUDITED', 'TRASHED') THEN emails.status 
                    ELSE excluded.status 
                END,
                ai_decision = COALESCE(emails.ai_decision, excluded.ai_decision),
                ai_reason = COALESCE(emails.ai_reason, excluded.ai_reason),
                validator_decision = COALESCE(emails.validator_decision, excluded.validator_decision),
                validator_reason = COALESCE(emails.validator_reason, excluded.validator_reason),
                revalidation_status = COALESCE(emails.revalidation_status, excluded.revalidation_status),
                revalidation_notes = COALESCE(emails.revalidation_notes, excluded.revalidation_notes),
                final_action = COALESCE(emails.final_action, excluded.final_action),
                last_run_id = COALESCE(excluded.last_run_id, emails.last_run_id),
                updated_at = CURRENT_TIMESTAMP
        """

        records = []
        for r in rows:
            uid_val = int(r["uid"])
            is_starred = 1 if str(r.get("is_starred", "FALSE")).upper() == "TRUE" else 0
            is_reply = 1 if str(r.get("is_reply", "FALSE")).upper() == "TRUE" else 0

            records.append({
                "account": r.get("account") or self.account,
                "uid": uid_val,
                "message_id": r.get("message_id") or "",
                "date": r.get("date") or "",
                "sender": r.get("sender") or r.get("from") or "",
                "subject": r.get("subject") or "",
                "snippet": r.get("snippet") or "",
                "is_starred": is_starred,
                "is_reply": is_reply,
                "status": r.get("status") or "FETCHED",
                "ai_decision": r.get("ai_decision"),
                "ai_reason": r.get("ai_reason"),
                "validator_decision": r.get("validator_decision"),
                "validator_reason": r.get("validator_reason"),
                "revalidation_status": r.get("revalidation_status"),
                "revalidation_notes": r.get("revalidation_notes"),
                "final_action": r.get("final_action"),
                "last_run_id": r.get("last_run_id") or r.get("run_id"),
            })

        with self.get_connection() as conn:
            conn.executemany(sql, records)
        elapsed = time.time() - t0
        logger.debug(f"Upserted {len(records)} emails into SQLite DB in {elapsed:.3f}s")
        return len(records)

    # -------------------------------------------------------------------------
    # READ / QUERY
    # -------------------------------------------------------------------------

    def get_emails_for_scan(self, limit: Optional[int] = None, only_kept: bool = False) -> List[Dict[str, Any]]:
        """
        Fetches emails needing AI classification.
        If only_kept is True, fetches emails where final_action = 'KEEP' for re-triage.
        """
        with self.get_connection() as conn:
            if only_kept:
                sql = """
                    SELECT * FROM emails 
                    WHERE account = ? AND final_action = 'KEEP' AND is_starred = 0 AND is_reply = 0
                    ORDER BY uid ASC
                """
                params: tuple = (self.account,)
            else:
                sql = """
                    SELECT * FROM emails 
                    WHERE account = ? AND status = 'FETCHED' AND is_starred = 0 AND is_reply = 0
                    ORDER BY uid ASC
                """
                params = (self.account,)

            if limit:
                sql += f" LIMIT {int(limit)}"

            cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_emails_for_audit(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetches candidate deletions needing Safety Auditor validation."""
        with self.get_connection() as conn:
            sql = """
                SELECT * FROM emails 
                WHERE account = ? AND status = 'SCANNED' AND final_action = 'DELETE'
                ORDER BY uid ASC
            """
            if limit:
                sql += f" LIMIT {int(limit)}"
            cursor = conn.execute(sql, (self.account,))
            return [dict(row) for row in cursor.fetchall()]

    def get_confirmed_deletions(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetches all emails confirmed for deletion that are not yet trashed."""
        with self.get_connection() as conn:
            sql = """
                SELECT * FROM emails 
                WHERE account = ? AND final_action = 'DELETE' AND status != 'TRASHED'
                ORDER BY uid ASC
            """
            if limit:
                sql += f" LIMIT {int(limit)}"
            cursor = conn.execute(sql, (self.account,))
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Returns aggregate pipeline statistics for the active account."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) AS total_emails,
                    COALESCE(SUM(CASE WHEN status = 'FETCHED' THEN 1 ELSE 0 END), 0) AS fetched,
                    COALESCE(SUM(CASE WHEN status = 'SCANNED' THEN 1 ELSE 0 END), 0) AS scanned,
                    COALESCE(SUM(CASE WHEN status = 'AUDITED' THEN 1 ELSE 0 END), 0) AS audited,
                    COALESCE(SUM(CASE WHEN status = 'TRASHED' THEN 1 ELSE 0 END), 0) AS trashed,
                    COALESCE(SUM(CASE WHEN final_action = 'DELETE' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS pending_delete,
                    COALESCE(SUM(CASE WHEN final_action = 'KEEP' THEN 1 ELSE 0 END), 0) AS kept,
                    COALESCE(SUM(CASE WHEN is_starred = 1 THEN 1 ELSE 0 END), 0) AS starred,
                    COALESCE(SUM(CASE WHEN is_reply = 1 THEN 1 ELSE 0 END), 0) AS replies
                FROM emails
                WHERE account = ?
            """, (self.account,))
            row = cursor.fetchone()
            return dict(row) if row else {}

    def query(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Executes a custom SELECT query and returns rows as a list of dicts."""
        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return [dict(r) for r in cursor.fetchall()]

    # -------------------------------------------------------------------------
    # UPDATE
    # -------------------------------------------------------------------------

    def update_scan_batch(self, scan_results: List[Dict[str, Any]]) -> int:
        """
        Updates batch of emails with Stage 2 AI Classifier decisions.
        Expected items: {"id" or "uid": <uid>, "ai_decision": "DELETE"/"KEEP", "ai_reason": "..."}
        """
        if not scan_results:
            return 0

        sql = """
            UPDATE emails SET
                ai_decision = :ai_decision,
                ai_reason = :ai_reason,
                final_action = :ai_decision,
                status = 'SCANNED',
                updated_at = CURRENT_TIMESTAMP
            WHERE account = :account AND uid = :uid
        """
        params = []
        for r in scan_results:
            uid_val = int(r.get("id") or r.get("uid"))
            dec = (r.get("ai_decision") or "KEEP").strip().upper()
            params.append({
                "account": self.account,
                "uid": uid_val,
                "ai_decision": dec,
                "ai_reason": r.get("ai_reason", "Unclassified"),
            })

        with self.get_connection() as conn:
            conn.executemany(sql, params)
        logger.debug(f"Updated scan batch of {len(params)} emails in DB")
        return len(params)

    def update_audit_batch(self, audit_results: List[Dict[str, Any]]) -> int:
        """
        Updates batch of emails with Stage 3 Safety Auditor decisions.
        Expected items: {"id" or "uid": <uid>, "validator_decision": "CONFIRMED_DELETE"/"OVERRIDE_KEEP", "validator_reason": "..."}
        """
        if not audit_results:
            return 0

        sql = """
            UPDATE emails SET
                validator_decision = :val_dec,
                validator_reason = :val_rsn,
                revalidation_status = :val_dec,
                revalidation_notes = :val_rsn,
                final_action = CASE WHEN :val_dec = 'OVERRIDE_KEEP' THEN 'KEEP' ELSE 'DELETE' END,
                status = 'AUDITED',
                updated_at = CURRENT_TIMESTAMP
            WHERE account = :account AND uid = :uid
        """
        params = []
        for r in audit_results:
            uid_val = int(r.get("id") or r.get("uid"))
            val_dec = (r.get("validator_decision") or "CONFIRMED_DELETE").strip().upper()
            val_rsn = r.get("validator_reason", "Audited")
            params.append({
                "account": self.account,
                "uid": uid_val,
                "val_dec": val_dec,
                "val_rsn": val_rsn,
            })

        with self.get_connection() as conn:
            conn.executemany(sql, params)
        logger.debug(f"Updated audit batch of {len(params)} emails in DB")
        return len(params)

    def mark_trashed(self, uids: List[Union[int, str]]) -> int:
        """Marks a list of UIDs as successfully moved to Gmail Trash."""
        if not uids:
            return 0

        int_uids = [int(u) for u in uids]
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in int_uids)
            sql = f"""
                UPDATE emails SET
                    status = 'TRASHED',
                    trashed_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid IN ({placeholders})
            """
            cursor = conn.execute(sql, (self.account, *int_uids))
            count = cursor.rowcount
            logger.info(f"Marked {count} emails as TRASHED in DB")
            return count

    def mark_restored(self, uids: List[Union[int, str]]) -> int:
        """Marks a list of UIDs as restored back to Inbox."""
        if not uids:
            return 0

        int_uids = [int(u) for u in uids]
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in int_uids)
            sql = f"""
                UPDATE emails SET
                    status = 'RESTORED',
                    trashed_at = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid IN ({placeholders})
            """
            cursor = conn.execute(sql, (self.account, *int_uids))
            count = cursor.rowcount
            logger.info(f"Marked {count} emails as RESTORED in DB")
            return count

    def reset_kept_for_rescan(self) -> int:
        """
        Resets emails previously marked as KEEP back to 'FETCHED' status
        so they can be re-evaluated with updated prompts without refetching from Gmail.
        """
        with self.get_connection() as conn:
            sql = """
                UPDATE emails SET
                    status = 'FETCHED',
                    ai_decision = NULL,
                    ai_reason = NULL,
                    validator_decision = NULL,
                    validator_reason = NULL,
                    revalidation_status = NULL,
                    revalidation_notes = NULL,
                    final_action = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND final_action = 'KEEP' AND is_starred = 0 AND is_reply = 0
            """
            cursor = conn.execute(sql, (self.account,))
            count = cursor.rowcount
            logger.info(f"Reset {count} KEPT emails for rescan in DB")
            return count

    def set_manual_override(self, uid: Union[int, str], action: str, note: str = "Manual UI override") -> bool:
        """Manually flips an email's final action between KEEP and DELETE."""
        action = action.strip().upper()
        if action not in ("KEEP", "DELETE"):
            raise ValueError(f"Action must be 'KEEP' or 'DELETE', got '{action}'")

        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE emails SET
                    final_action = ?,
                    revalidation_status = ?,
                    revalidation_notes = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid = ?
            """, (action, f"MANUAL_{action}", note, self.account, int(uid)))
            affected = cursor.rowcount > 0
            if affected:
                logger.info(f"Manual override applied: UID {uid} -> {action} ({note})")
            return affected

    def get_emails_page(
        self,
        search: str = "",
        action_filter: str = "ALL",
        status_filter: str = "ALL",
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fast paginated query with multi-field search and status filters for UI explorer.
        Returns (rows, total_matching_count).
        """
        conditions = ["account = ?"]
        params: List[Any] = [self.account]

        if action_filter and action_filter.upper() != "ALL":
            conditions.append("final_action = ?")
            params.append(action_filter.upper())

        if status_filter and status_filter.upper() != "ALL":
            conditions.append("status = ?")
            params.append(status_filter.upper())

        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append("(sender LIKE ? OR subject LIKE ? OR snippet LIKE ? OR CAST(uid AS TEXT) LIKE ? OR last_run_id LIKE ?)")
            params.extend([term, term, term, term, term])

        where_clause = " AND ".join(conditions)

        with self.get_connection() as conn:
            # Count query
            count_sql = f"SELECT COUNT(*) FROM emails WHERE {where_clause}"
            total_count = conn.execute(count_sql, tuple(params)).fetchone()[0]

            # Data query
            data_sql = f"""
                SELECT uid, date, sender, subject, snippet, is_starred, is_reply,
                       status, ai_decision, ai_reason, validator_decision, validator_reason,
                       final_action, revalidation_status, revalidation_notes, last_run_id, updated_at
                FROM emails
                WHERE {where_clause}
                ORDER BY uid DESC
                LIMIT ? OFFSET ?
            """
            data_params = list(params) + [int(limit), int(offset)]
            cursor = conn.execute(data_sql, tuple(data_params))
            rows = [dict(r) for r in cursor.fetchall()]

            return rows, total_count

    def clean_existing_snippets(self) -> int:
        """
        Cleans existing email snippets in the database that contain raw MIME boundaries or headers.
        Returns the count of updated emails.
        """
        from gmail_cleaner.imap_client import clean_raw_text_snippet
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT uid, snippet FROM emails
                WHERE account = ? AND (
                    snippet LIKE '%Content-Type:%' OR
                    snippet LIKE '%------=%' OR
                    snippet LIKE '%Content-Transfer-Encoding%' OR
                    snippet LIKE '%--=%'
                )
            """, (self.account,))
            rows = cursor.fetchall()
            if not rows:
                return 0

            updates = []
            for r in rows:
                cleaned = clean_raw_text_snippet(r["snippet"])
                updates.append((cleaned, self.account, r["uid"]))

            conn.executemany("""
                UPDATE emails SET snippet = ?, updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid = ?
            """, updates)
            logger.info(f"Cleaned {len(updates)} snippets in SQLite database")
            return len(updates)

    def backfill_missing_snippets(self, batch_size: int = 100, limit: Optional[int] = None) -> int:
        """
        Connects to Gmail IMAP and fetches 10KB body slices for emails in the DB
        that currently have an empty snippet. Updates the DB in batches.
        Reports progress to background worker and respects cooperative cancellation.
        """
        from gmail_cleaner.imap_client import connect_imap, fetch_snippets_for_uids
        from gmail_cleaner.worker import worker

        with self.get_connection() as conn:
            query = """
                SELECT uid FROM emails
                WHERE account = ? AND (snippet IS NULL OR snippet = '' OR snippet = '(No snippet available)')
                ORDER BY uid DESC
            """
            rows = conn.execute(query, (self.account,)).fetchall()
            uids_to_fetch = [int(r["uid"]) for r in rows]

        if not uids_to_fetch:
            logger.info("No missing snippets to backfill.")
            return 0

        mail = connect_imap(email_user=self.account)
        status, _ = mail.select("INBOX")
        status, data = mail.uid("search", None, "ALL")
        inbox_uids = set(int(u) for u in data[0].decode().split()) if (status == "OK" and data and data[0]) else set()

        # Intersect with active INBOX UIDs
        active_uids_to_fetch = [u for u in uids_to_fetch if u in inbox_uids]
        if limit:
            active_uids_to_fetch = active_uids_to_fetch[:int(limit)]

        if not active_uids_to_fetch:
            logger.info("No active INBOX emails requiring snippet backfill.")
            mail.close()
            mail.logout()
            return 0

        logger.info(f"Starting snippet backfill for {len(active_uids_to_fetch)} active INBOX emails (batch size: {batch_size})...")
        total_updated = 0
        total_to_fetch = len(active_uids_to_fetch)
        worker.update_progress(0, total_to_fetch, f"Backfilling snippets: 0/{total_to_fetch}")

        try:
            for i in range(0, total_to_fetch, batch_size):
                if worker.is_cancel_requested:
                    logger.warning("Snippet backfill cancelled by user.")
                    break

                batch_uids = active_uids_to_fetch[i : i + batch_size]
                snippets_map = fetch_snippets_for_uids(mail, batch_uids, snippet_length=500)

                updates = []
                for u, snip in snippets_map.items():
                    if snip:
                        updates.append((snip, self.account, u))

                if updates:
                    with self.get_connection() as conn:
                        conn.executemany("""
                            UPDATE emails SET snippet = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE account = ? AND uid = ?
                        """, updates)
                    total_updated += len(updates)

                progress_msg = f"Backfilled snippets: {total_updated}/{total_to_fetch}"
                logger.info(f"   [Snippet Backfill] {progress_msg}")
                worker.update_progress(min(i + len(batch_uids), total_to_fetch), total_to_fetch, progress_msg)

        finally:
            try:
                mail.close()
                mail.logout()
            except Exception:
                pass

        logger.info(f"✅ Snippet backfill complete: {total_updated}/{total_to_fetch} updated.")
        return total_updated

    # -------------------------------------------------------------------------
    # IMPORT & EXPORT
    # -------------------------------------------------------------------------

    def import_from_csv(self, csv_path: str) -> int:
        """Loads an existing pipeline CSV (e.g. revalidated_*.csv) into SQLite."""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        rows = []
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for r in reader:
                # Map CSV column names
                r["sender"] = r.get("from") or r.get("sender") or ""
                final_act = (r.get("final_action") or "").strip().upper()
                val_dec = (r.get("validator_decision") or "").strip().upper()

                # Infer status from existing columns
                if final_act:
                    r["status"] = "AUDITED" if val_dec else "SCANNED"
                else:
                    r["status"] = "FETCHED"

                rows.append(r)

        upserted = self.upsert_emails(rows)
        logger.info(f"Imported {upserted} emails from CSV '{csv_path}' into SQLite DB")
        return upserted

    def export_to_csv(self, csv_path: str, final_action: Optional[str] = None,
                      status: Optional[str] = None) -> int:
        """Exports matching database rows to a CSV review artifact."""
        os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)

        conditions = ["account = ?"]
        params: List[Any] = [self.account]

        if final_action:
            conditions.append("final_action = ?")
            params.append(final_action.upper())

        if status:
            conditions.append("status = ?")
            params.append(status.upper())

        where_clause = " AND ".join(conditions)
        sql = f"SELECT * FROM emails WHERE {where_clause} ORDER BY uid ASC"

        fieldnames = [
            "uid", "message_id", "date", "from", "subject", "snippet",
            "is_starred", "is_reply", "ai_decision", "ai_reason",
            "validator_decision", "validator_reason",
            "revalidation_status", "revalidation_notes",
            "final_action"
        ]

        count = 0
        with self.get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, extrasaction="ignore")
                writer.writeheader()
                for row in cursor:
                    d = dict(row)
                    d["from"] = d.get("sender", "")
                    d["is_starred"] = "TRUE" if d.get("is_starred") else "FALSE"
                    d["is_reply"] = "TRUE" if d.get("is_reply") else "FALSE"
                    writer.writerow(d)
                    count += 1

        logger.info(f"Exported {count} emails from SQLite DB to CSV '{csv_path}'")
        return count

    # -------------------------------------------------------------------------
    # RUN HISTORY & LIFECYCLE TRACKING
    # -------------------------------------------------------------------------

    def create_run(
        self,
        action_type: str,
        params: Optional[Dict[str, Any]] = None,
        notes: str = "",
        run_id: Optional[str] = None,
    ) -> str:
        """
        Creates a new pipeline run record with status 'RUNNING'.
        Returns the unique run_id.
        """
        if not run_id:
            clean_type = re.sub(r"[^a-zA-Z0-9]+", "_", action_type.lower()).strip("_")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_id = f"run_{timestamp}_{clean_type}"

        params_json = json.dumps(params, default=str) if params else "{}"
        sql = """
            INSERT INTO runs (
                run_id, account, action_type, status, started_at, params_json, notes
            ) VALUES (
                ?, ?, ?, 'RUNNING', CURRENT_TIMESTAMP, ?, ?
            )
        """
        with self.get_connection() as conn:
            conn.execute(sql, (run_id, self.account, action_type, params_json, notes))
        logger.info(f"Created run record '{run_id}' for action '{action_type}'")
        return run_id

    def update_run(
        self,
        run_id: str,
        status: Optional[str] = None,
        **kwargs: Any,
    ) -> bool:
        """
        Updates an existing run record with status, counts, duration, and artifacts.
        Automatically calculates duration_seconds and sets completed_at if status is terminal.
        """
        updates = []
        params = []

        if status:
            status = status.upper()
            updates.append("status = ?")
            params.append(status)

            if status in ("COMPLETED", "FAILED", "CANCELLED") and "completed_at" not in kwargs:
                updates.append("completed_at = CURRENT_TIMESTAMP")

        for key, val in kwargs.items():
            if key in (
                "duration_seconds", "total_emails", "delete_count", "keep_count",
                "rescued_count", "trashed_count", "artifact_path", "error_message",
                "notes", "completed_at"
            ):
                updates.append(f"{key} = ?")
                params.append(val)
            elif key == "params" and isinstance(val, dict):
                updates.append("params_json = ?")
                params.append(json.dumps(val, default=str))

        if not updates:
            return False

        params.extend([self.account, run_id])
        sql = f"UPDATE runs SET {', '.join(updates)} WHERE account = ? AND run_id = ?"

        with self.get_connection() as conn:
            cursor = conn.execute(sql, tuple(params))
            
            # Post-check: ensure duration_seconds is computed if completed and not passed
            if status in ("COMPLETED", "FAILED", "CANCELLED") and "duration_seconds" not in kwargs:
                conn.execute("""
                    UPDATE runs 
                    SET duration_seconds = ROUND((JULIANDAY(COALESCE(completed_at, CURRENT_TIMESTAMP)) - JULIANDAY(started_at)) * 86400.0, 1)
                    WHERE account = ? AND run_id = ? AND (duration_seconds IS NULL OR duration_seconds = 0.0)
                """, (self.account, run_id))

            affected = cursor.rowcount > 0
            if affected:
                logger.debug(f"Updated run record '{run_id}' -> status: {status or 'updated'}")
            return affected

    def get_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns the most recent runs for this account ordered by start time."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM runs
                WHERE account = ?
                ORDER BY started_at DESC, rowid DESC
                LIMIT ?
            """, (self.account, int(limit)))
            return [dict(r) for r in cursor.fetchall()]

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Fetches metadata for a single run by ID."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM runs
                WHERE account = ? AND run_id = ?
            """, (self.account, run_id))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_latest_run(self) -> Optional[Dict[str, Any]]:
        """Returns the single most recent run record for Quick Status display."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM runs
                WHERE account = ?
                ORDER BY started_at DESC, rowid DESC
                LIMIT 1
            """, (self.account,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_run_metrics_summary(self) -> Dict[str, Any]:
        """Returns overall metrics across all runs for the dashboard."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) AS total_runs,
                    COALESCE(SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END), 0) AS completed_runs,
                    COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_runs,
                    COALESCE(SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END), 0) AS cancelled_runs,
                    COALESCE(SUM(total_emails), 0) AS total_emails_handled,
                    COALESCE(SUM(trashed_count), 0) AS total_emails_trashed
                FROM runs
                WHERE account = ?
            """, (self.account,))
            row = cursor.fetchone()
            return dict(row) if row else {}

    def backfill_historical_runs(self) -> int:
        """
        Discovers existing pipeline artifact CSVs in outputs/<account>/
        and creates historical run records so past work is immediately visible.
        """
        account_dir = get_account_dir(self.account)
        if not os.path.exists(account_dir):
            return 0

        pattern_reval = os.path.join(account_dir, "4_revalidate", "revalidated_*.csv")
        pattern_delete = os.path.join(account_dir, "5_processed", "completed_*.csv")
        csv_files = glob.glob(pattern_reval) + glob.glob(pattern_delete)

        if not csv_files:
            return 0

        existing_runs = {r["run_id"] for r in self.get_runs(limit=500)}
        added_count = 0

        for filepath in sorted(csv_files):
            fname = os.path.basename(filepath)
            match = re.search(r"(\d{8}_\d{6})", fname)
            ts_str = match.group(1) if match else "legacy"
            action_type = "Revalidation Review" if "revalidate" in filepath else "Gmail Trash Execution"
            run_id = f"run_hist_{'reval' if 'revalidate' in filepath else 'trash'}_{ts_str}"

            if run_id in existing_runs:
                continue

            total_emails = 0
            del_count = 0
            keep_count = 0
            try:
                with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        total_emails += 1
                        act = (r.get("final_action") or "").strip().upper()
                        if act == "DELETE":
                            del_count += 1
                        else:
                            keep_count += 1
            except Exception:
                pass

            started_ts = None
            if match:
                try:
                    dt = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                    started_ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    pass

            with self.get_connection() as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO runs (
                        run_id, account, action_type, status, started_at, completed_at,
                        duration_seconds, total_emails, delete_count, keep_count,
                        trashed_count, artifact_path, notes
                    ) VALUES (
                        ?, ?, ?, 'COMPLETED', COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP),
                        0.0, ?, ?, ?, ?, ?, 'Imported from historical pipeline CSV artifact'
                    )
                """, (
                    run_id, self.account, action_type, started_ts, started_ts,
                    total_emails, del_count, keep_count,
                    del_count if "completed" in fname else 0,
                    filepath
                ))
                added_count += 1
                existing_runs.add(run_id)

        if added_count > 0:
            logger.info(f"Backfilled {added_count} historical run records from artifacts")
        return added_count

