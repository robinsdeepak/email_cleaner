"""
SQLite database storage and state management for Gmail AI Cleaner.

Provides thread-safe connections, WAL mode concurrency, schema migrations,
and unified CRUD operations across all pipeline stages.
"""

from contextlib import contextmanager
import csv
from datetime import datetime
import os
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
        """Initializes tables, constraints, and performance indexes."""
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
                    
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    trashed_at          TIMESTAMP,
                    
                    PRIMARY KEY (account, uid)
                );

                CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(account, status);
                CREATE INDEX IF NOT EXISTS idx_emails_final_action ON emails(account, final_action);
                CREATE INDEX IF NOT EXISTS idx_emails_message_id ON emails(account, message_id);
            """)
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
                revalidation_notes, final_action, updated_at
            ) VALUES (
                :account, :uid, :message_id, :date, :sender, :subject, :snippet,
                :is_starred, :is_reply, :status, :ai_decision, :ai_reason,
                :validator_decision, :validator_reason, :revalidation_status,
                :revalidation_notes, :final_action, CURRENT_TIMESTAMP
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
            conditions.append("(sender LIKE ? OR subject LIKE ? OR snippet LIKE ? OR CAST(uid AS TEXT) LIKE ?)")
            params.extend([term, term, term, term])

        where_clause = " AND ".join(conditions)

        with self.get_connection() as conn:
            # Count query
            count_sql = f"SELECT COUNT(*) FROM emails WHERE {where_clause}"
            total_count = conn.execute(count_sql, tuple(params)).fetchone()[0]

            # Data query
            data_sql = f"""
                SELECT uid, date, sender, subject, snippet, is_starred, is_reply,
                       status, ai_decision, ai_reason, validator_decision, validator_reason,
                       final_action, revalidation_status, revalidation_notes, updated_at
                FROM emails
                WHERE {where_clause}
                ORDER BY uid DESC
                LIMIT ? OFFSET ?
            """
            data_params = list(params) + [int(limit), int(offset)]
            cursor = conn.execute(data_sql, tuple(data_params))
            rows = [dict(r) for r in cursor.fetchall()]

            return rows, total_count

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
