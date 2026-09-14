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

logger = get_logger("db")


def get_default_db_path(email_addr: Optional[str] = None) -> str:
    """Returns the centralized SQLite database path (defaults to ./emails.db at root)."""
    env_path = os.getenv("EMAILS_DB_PATH")
    if env_path:
        return os.path.abspath(env_path)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base_dir, "emails.db")


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
                CREATE TABLE IF NOT EXISTS accounts (
                    email               TEXT PRIMARY KEY,
                    display_name        TEXT,
                    app_password        TEXT NOT NULL,
                    is_default          BOOLEAN DEFAULT 0,
                    last_uid_scanned    INTEGER DEFAULT 0,
                    uid_validity        TEXT,
                    last_fetched_at     TIMESTAMP,
                    total_scanned       INTEGER DEFAULT 0,
                    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

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
                    ai_confidence       TEXT,
                    ai_category         TEXT,
                    ai_reason           TEXT,
                    validator_decision  TEXT,
                    validator_confidence TEXT,
                    validator_reason    TEXT,
                    revalidation_status TEXT,
                    revalidation_notes  TEXT,
                    final_action        TEXT,
                    is_reviewed         BOOLEAN DEFAULT 0,
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

            # Migration: Ensure new columns exist if table was created in older schema
            cursor = conn.execute("PRAGMA table_info(accounts);")
            acc_col_names = [col["name"] for col in cursor.fetchall()]
            if "uid_validity" not in acc_col_names:
                conn.execute("ALTER TABLE accounts ADD COLUMN uid_validity TEXT;")
                logger.info("Migrated accounts table: added 'uid_validity' column")

            cursor = conn.execute("PRAGMA table_info(emails);")
            col_names = [col["name"] for col in cursor.fetchall()]
            if "last_run_id" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN last_run_id TEXT;")
                logger.info("Migrated emails table: added 'last_run_id' column")
            if "ai_confidence" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN ai_confidence TEXT;")
                logger.info("Migrated emails table: added 'ai_confidence' column")
            if "ai_category" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN ai_category TEXT;")
                logger.info("Migrated emails table: added 'ai_category' column")
            if "validator_confidence" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN validator_confidence TEXT;")
                logger.info("Migrated emails table: added 'validator_confidence' column")
            if "is_reviewed" not in col_names:
                conn.execute("ALTER TABLE emails ADD COLUMN is_reviewed BOOLEAN DEFAULT 0;")
                logger.info("Migrated emails table: added 'is_reviewed' column")

            conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_ai_decision ON emails(account, ai_decision);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_review ON emails(account, final_action, is_reviewed);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_category ON emails(account, ai_category);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_emails_last_run_id ON emails(account, last_run_id);")

            self.seed_accounts_from_env(conn)

        logger.debug(f"SQLite DB initialized with WAL mode at: {self.db_path}")

    def reset_database(self, reset_cursor: bool = True) -> None:
        """Cleans all emails and runs from SQLite DB, recreating clean empty tables."""
        with self.get_connection() as conn:
            conn.execute("DROP TABLE IF EXISTS emails;")
            conn.execute("DROP TABLE IF EXISTS runs;")
            if reset_cursor:
                conn.execute("""
                    UPDATE accounts 
                    SET last_uid_scanned = 0, total_scanned = 0, last_fetched_at = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE email = ?;
                """, (self.account.strip().lower(),))
        self.init_db()
        logger.info(f"Cleaned and reinitialized fresh SQLite DB at {self.db_path}")

    # -------------------------------------------------------------------------
    # ACCOUNTS & CREDENTIALS MANAGEMENT
    # -------------------------------------------------------------------------

    def seed_accounts_from_env(self, conn=None):
        """Auto-seeds the default account into `accounts` table from .env if table is empty."""
        from gmail_cleaner.config import GMAIL_USER, GMAIL_APP_PASSWORD
        user = (GMAIL_USER or "").strip()
        pwd = (GMAIL_APP_PASSWORD or "").strip()
        placeholders = {"your-email@gmail.com", "your_email@gmail.com", ""}
        pwd_placeholders = {"your-16-char-app-password", "xxxx-xxxx-xxxx-xxxx", ""}
        if not user or user in placeholders or not pwd or pwd in pwd_placeholders:
            return

        def _do_seed(c):
            row = c.execute("SELECT COUNT(*) FROM accounts").fetchone()
            if row and row[0] == 0:
                c.execute("""
                    INSERT OR IGNORE INTO accounts (email, display_name, app_password, is_default)
                    VALUES (?, ?, ?, 1)
                """, (user, "Personal Gmail", pwd))
                logger.info(f"Auto-seeded default account '{user}' from .env into SQLite accounts table.")

        if conn:
            _do_seed(conn)
        else:
            with self.get_connection() as c:
                _do_seed(c)

    def add_or_update_account(
        self,
        email: str,
        app_password: str,
        display_name: Optional[str] = None,
        is_default: bool = False
    ) -> None:
        """Adds or updates an email account with its credentials in SQLite."""
        email = email.strip().lower()
        pwd = app_password.strip()
        name = (display_name or "").strip() or email.split("@")[0].title()

        with self.get_connection() as conn:
            count = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            if count == 0:
                is_default = True

            if is_default:
                conn.execute("UPDATE accounts SET is_default = 0;")

            conn.execute("""
                INSERT INTO accounts (email, display_name, app_password, is_default, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(email) DO UPDATE SET
                    display_name = excluded.display_name,
                    app_password = excluded.app_password,
                    is_default = CASE WHEN excluded.is_default = 1 THEN 1 ELSE accounts.is_default END,
                    updated_at = CURRENT_TIMESTAMP;
            """, (email, name, pwd, 1 if is_default else 0))
            logger.info(f"Saved account '{email}' (default={is_default}) to SQLite accounts table.")

    def get_account(self, email: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetches account details for a specific email or the default active account."""
        with self.get_connection() as conn:
            if email:
                cursor = conn.execute("SELECT * FROM accounts WHERE email = ?", (email.strip().lower(),))
            else:
                cursor = conn.execute("SELECT * FROM accounts WHERE is_default = 1 LIMIT 1")
            row = cursor.fetchone()
            if not row and not email:
                cursor = conn.execute("SELECT * FROM accounts ORDER BY rowid ASC LIMIT 1")
                row = cursor.fetchone()
            return dict(row) if row else None

    def get_account_credentials(self, email: Optional[str] = None) -> Optional[Tuple[str, str]]:
        """Returns (email, app_password) for the requested account or default account."""
        acc = self.get_account(email)
        if acc and acc.get("app_password"):
            return acc["email"], acc["app_password"]
        return None

    def list_accounts(self) -> List[Dict[str, Any]]:
        """Lists all configured email accounts from SQLite."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT email, display_name, is_default, last_uid_scanned, last_fetched_at, total_scanned, created_at, updated_at
                FROM accounts
                ORDER BY is_default DESC, email ASC
            """)
            return [dict(r) for r in cursor.fetchall()]

    def set_default_account(self, email: str) -> None:
        """Sets the specified email as the default account."""
        email = email.strip().lower()
        with self.get_connection() as conn:
            conn.execute("UPDATE accounts SET is_default = 0;")
            conn.execute("UPDATE accounts SET is_default = 1 WHERE email = ?;", (email,))
            logger.info(f"Set default account to: {email}")

    def delete_account(self, email: str) -> bool:
        """Deletes an account from the accounts table."""
        email = email.strip().lower()
        with self.get_connection() as conn:
            was_default = conn.execute("SELECT is_default FROM accounts WHERE email = ?", (email,)).fetchone()
            cursor = conn.execute("DELETE FROM accounts WHERE email = ?", (email,))
            deleted = cursor.rowcount > 0
            if deleted and was_default and was_default[0]:
                conn.execute("UPDATE accounts SET is_default = 1 WHERE rowid = (SELECT rowid FROM accounts LIMIT 1);")
            return deleted

    def get_account_cursor(self, account: Optional[str] = None) -> int:
        """Gets last_uid_scanned cursor for an account."""
        acc_email = (account or self.account).strip().lower()
        with self.get_connection() as conn:
            row = conn.execute("SELECT last_uid_scanned FROM accounts WHERE email = ?", (acc_email,)).fetchone()
            return int(row[0]) if row and row[0] is not None else 0

    def update_account_cursor(
        self,
        account: str,
        last_uid: int,
        total_scanned: Optional[int] = None,
        uid_validity: Optional[str] = None
    ) -> None:
        """Updates last_uid_scanned, total_scanned, and uid_validity for an account."""
        acc_email = account.strip().lower()
        with self.get_connection() as conn:
            updates = ["last_uid_scanned = ?", "last_fetched_at = CURRENT_TIMESTAMP", "updated_at = CURRENT_TIMESTAMP"]
            params: List[Any] = [int(last_uid)]
            if total_scanned is not None:
                updates.append("total_scanned = ?")
                params.append(int(total_scanned))
            if uid_validity is not None:
                updates.append("uid_validity = ?")
                params.append(str(uid_validity))
            params.append(acc_email)
            sql = f"UPDATE accounts SET {', '.join(updates)} WHERE email = ?"
            conn.execute(sql, tuple(params))

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
                is_starred, is_reply, status, ai_decision, ai_confidence, ai_category, ai_reason,
                validator_decision, validator_confidence, validator_reason, revalidation_status,
                revalidation_notes, final_action, is_reviewed, last_run_id, updated_at
            ) VALUES (
                :account, :uid, :message_id, :date, :sender, :subject, :snippet,
                :is_starred, :is_reply, :status, :ai_decision, :ai_confidence, :ai_category, :ai_reason,
                :validator_decision, :validator_confidence, :validator_reason, :revalidation_status,
                :revalidation_notes, :final_action, :is_reviewed, :last_run_id, CURRENT_TIMESTAMP
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
                ai_confidence = COALESCE(emails.ai_confidence, excluded.ai_confidence),
                ai_category = COALESCE(emails.ai_category, excluded.ai_category),
                ai_reason = COALESCE(emails.ai_reason, excluded.ai_reason),
                validator_decision = COALESCE(emails.validator_decision, excluded.validator_decision),
                validator_confidence = COALESCE(emails.validator_confidence, excluded.validator_confidence),
                validator_reason = COALESCE(emails.validator_reason, excluded.validator_reason),
                revalidation_status = COALESCE(emails.revalidation_status, excluded.revalidation_status),
                revalidation_notes = COALESCE(emails.revalidation_notes, excluded.revalidation_notes),
                final_action = COALESCE(emails.final_action, excluded.final_action),
                is_reviewed = COALESCE(emails.is_reviewed, excluded.is_reviewed),
                last_run_id = COALESCE(NULLIF(excluded.last_run_id, ''), emails.last_run_id),
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
                "ai_decision": r.get("ai_decision") or r.get("status"),
                "ai_confidence": r.get("ai_confidence") or r.get("confidence"),
                "ai_category": r.get("ai_category") or r.get("category"),
                "ai_reason": r.get("ai_reason") or r.get("reason"),
                "validator_decision": r.get("validator_decision") or r.get("validator_status"),
                "validator_confidence": r.get("validator_confidence"),
                "validator_reason": r.get("validator_reason"),
                "revalidation_status": r.get("revalidation_status"),
                "revalidation_notes": r.get("revalidation_notes"),
                "final_action": r.get("final_action"),
                "is_reviewed": 1 if r.get("is_reviewed") else 0,
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

    def get_deletions_breakdown(self) -> Dict[str, int]:
        """Returns counts of non-trashed emails broken down by deletable status categories."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN (ai_decision = 'CONFIDENT_DELETE' OR validator_decision = 'CONFIRMED_DELETE') AND final_action = 'DELETE' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS confident_delete,
                    COALESCE(SUM(CASE WHEN (ai_decision = 'PROBABLE_DELETE' OR validator_decision = 'SOFT_DELETE') AND final_action = 'DELETE' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS probable_delete,
                    COALESCE(SUM(CASE WHEN (final_action = 'REVIEW' OR ai_decision = 'NEEDS_REVIEW' OR validator_decision IN ('NEEDS_USER_REVIEW', 'NEEDS_REVIEW')) AND status != 'TRASHED' AND final_action != 'KEEP' THEN 1 ELSE 0 END), 0) AS needs_review,
                    COALESCE(SUM(CASE WHEN final_action = 'DELETE' AND status != 'TRASHED' AND (ai_decision IS NULL OR ai_decision = '' OR (is_reviewed = 1 AND ai_decision NOT IN ('CONFIDENT_DELETE', 'PROBABLE_DELETE'))) THEN 1 ELSE 0 END), 0) AS manual_delete
                FROM emails
                WHERE account = ?
            """, (self.account,))
            row = cursor.fetchone()
            return dict(row) if row else {
                "confident_delete": 0,
                "probable_delete": 0,
                "needs_review": 0,
                "manual_delete": 0,
            }

    def get_confirmed_deletions(
        self,
        statuses: Optional[List[str]] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetches emails confirmed for deletion that are not yet trashed.
        If statuses is provided, filters to match selected status categories:
          - 'CONFIDENT_DELETE'
          - 'PROBABLE_DELETE'
          - 'NEEDS_REVIEW'
          - 'MANUAL_DELETE'
        """
        with self.get_connection() as conn:
            if statuses is None:
                sql = """
                    SELECT * FROM emails 
                    WHERE account = ? AND final_action = 'DELETE' AND status != 'TRASHED'
                    ORDER BY uid ASC
                """
                params: List[Any] = [self.account]
            elif len(statuses) == 0:
                return []
            else:
                clauses = []
                for s in statuses:
                    s_upper = s.strip().upper()
                    if s_upper == "CONFIDENT_DELETE":
                        clauses.append("((ai_decision = 'CONFIDENT_DELETE' OR validator_decision = 'CONFIRMED_DELETE') AND final_action = 'DELETE')")
                    elif s_upper == "PROBABLE_DELETE":
                        clauses.append("((ai_decision = 'PROBABLE_DELETE' OR validator_decision = 'SOFT_DELETE') AND final_action = 'DELETE')")
                    elif s_upper == "NEEDS_REVIEW":
                        clauses.append("((final_action = 'REVIEW' OR ai_decision = 'NEEDS_REVIEW' OR validator_decision IN ('NEEDS_USER_REVIEW', 'NEEDS_REVIEW')) AND final_action != 'KEEP')")
                    elif s_upper == "MANUAL_DELETE":
                        clauses.append("(final_action = 'DELETE' AND (ai_decision IS NULL OR ai_decision = '' OR (is_reviewed = 1 AND ai_decision NOT IN ('CONFIDENT_DELETE', 'PROBABLE_DELETE'))))")
                    elif s_upper in ("DELETE", "ALL"):
                        clauses.append("(final_action = 'DELETE')")

                if not clauses:
                    return []

                status_sql = " OR ".join(clauses)
                sql = f"""
                    SELECT * FROM emails 
                    WHERE account = ? AND status != 'TRASHED' AND ({status_sql})
                    ORDER BY uid ASC
                """
                params = [self.account]

            if limit:
                sql += f" LIMIT {int(limit)}"
            cursor = conn.execute(sql, tuple(params))
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
                    COALESCE(SUM(CASE WHEN final_action = 'REVIEW' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS needs_review,
                    COALESCE(SUM(CASE WHEN ai_decision = 'CONFIDENT_DELETE' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS confident_delete,
                    COALESCE(SUM(CASE WHEN ai_decision = 'PROBABLE_DELETE' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) AS probable_delete,
                    COALESCE(SUM(CASE WHEN ai_decision = 'CONFIDENT_KEEP' THEN 1 ELSE 0 END), 0) AS confident_keep,
                    COALESCE(SUM(CASE WHEN is_starred = 1 THEN 1 ELSE 0 END), 0) AS starred,
                    COALESCE(SUM(CASE WHEN is_reply = 1 THEN 1 ELSE 0 END), 0) AS replies
                FROM emails
                WHERE account = ?
            """, (self.account,))
            row = cursor.fetchone()
            return dict(row) if row else {}

    def get_category_stats(self) -> Dict[str, int]:
        """Returns email counts grouped by ai_category for the active account."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT COALESCE(ai_category, 'UNCLASSIFIED') as category, COUNT(*) as cnt
                FROM emails
                WHERE account = ?
                GROUP BY ai_category
                ORDER BY cnt DESC
            """, (self.account,))
            return {row["category"]: row["cnt"] for row in cursor.fetchall()}

    def get_sender_clusters(self, limit: int = 50, search: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Returns top email senders grouped by email frequency with AI status breakdowns.
        
        Fields per cluster:
        - sender: clean sender string
        - total: total emails from sender
        - delete_count: emails with final_action = 'DELETE' or status = 'CONFIDENT_DELETE'
        - keep_count: emails with final_action = 'KEEP'
        - review_count: emails with final_action = 'REVIEW'
        - trashed_count: emails already trashed
        - sample_subjects: up to 3 distinct sample subjects
        - top_category: most frequent ai_category
        """
        with self.get_connection() as conn:
            where_clause = "WHERE account = ?"
            params: List[Any] = [self.account]
            if search:
                where_clause += " AND (sender LIKE ? OR subject LIKE ?)"
                search_param = f"%{search.strip()}%"
                params.extend([search_param, search_param])
            
            sql = f"""
                SELECT
                    COALESCE(NULLIF(TRIM(sender), ''), 'Unknown Sender') as clean_sender,
                    COUNT(*) as total,
                    COALESCE(SUM(CASE WHEN final_action = 'DELETE' OR ai_decision = 'CONFIDENT_DELETE' THEN 1 ELSE 0 END), 0) as delete_count,
                    COALESCE(SUM(CASE WHEN final_action = 'KEEP' THEN 1 ELSE 0 END), 0) as keep_count,
                    COALESCE(SUM(CASE WHEN final_action = 'REVIEW' AND status != 'TRASHED' THEN 1 ELSE 0 END), 0) as review_count,
                    COALESCE(SUM(CASE WHEN status = 'TRASHED' THEN 1 ELSE 0 END), 0) as trashed_count,
                    GROUP_CONCAT(DISTINCT subject) as all_subjects,
                    COALESCE(MAX(ai_category), 'OTHER') as sample_category
                FROM emails
                {where_clause}
                GROUP BY clean_sender
                ORDER BY total DESC
                LIMIT ?
            """
            params.append(limit)
            cursor = conn.execute(sql, params)
            results = []
            for row in cursor.fetchall():
                subjs_raw = row["all_subjects"] or ""
                subjs = [s.strip() for s in subjs_raw.split(",") if s.strip()][:3]
                results.append({
                    "sender": row["clean_sender"],
                    "total": row["total"],
                    "delete_count": row["delete_count"],
                    "keep_count": row["keep_count"],
                    "review_count": row["review_count"],
                    "trashed_count": row["trashed_count"],
                    "sample_subjects": subjs,
                    "top_category": row["sample_category"],
                })
            return results

    def bulk_override_by_sender(self, sender: str, action: str, run_id: Optional[str] = None) -> int:
        """
        Applies a KEEP or DELETE manual override to all non-trashed emails matching the specified sender.
        Stamps a run_id for auditing.
        """
        action = action.strip().upper()
        if action not in ("KEEP", "DELETE"):
            raise ValueError(f"Action must be 'KEEP' or 'DELETE', got '{action}'")
        
        if not run_id:
            run_id = self.create_run(action_type=f"Sender Override: {action} ({sender})")
            
        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE emails SET
                    final_action = ?,
                    is_reviewed = 1,
                    revalidation_status = ?,
                    revalidation_notes = ?,
                    last_run_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND sender = ? AND status != 'TRASHED'
            """, (
                action,
                f"SENDER_OVERRIDE_{action}",
                f"Bulk override by sender '{sender}' -> {action}",
                run_id,
                self.account,
                sender,
            ))
            count = cursor.rowcount
            logger.info(f"Updated {count} emails for sender '{sender}' to {action} (Run ID: {run_id})")
            
        if count > 0:
            del_c = count if action == "DELETE" else 0
            keep_c = count if action == "KEEP" else 0
            self.update_run(run_id, status="COMPLETED", total_emails=count, delete_count=del_c, keep_count=keep_c)
        else:
            self.update_run(run_id, status="COMPLETED", total_emails=0)
        return count

    def approve_confident_deletions(self, run_id: Optional[str] = None) -> int:
        """Confirms all CONFIDENT_DELETE emails that are currently pending into final_action = 'DELETE' with is_reviewed = 1 and stamps run_id."""
        if not run_id:
            run_id = self.create_run(action_type="Approve Confident Deletions")
        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE emails SET
                    final_action = 'DELETE',
                    is_reviewed = 1,
                    revalidation_status = 'APPROVED_DELETE',
                    revalidation_notes = 'Batch approved confident deletion via UI',
                    last_run_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND ai_decision = 'CONFIDENT_DELETE' AND status != 'TRASHED'
            """, (run_id, self.account,))
            count = cursor.rowcount
            logger.info(f"Approved {count} confident deletions in DB (Run ID: {run_id})")
        if count > 0:
            self.update_run(run_id, status="COMPLETED", total_emails=count, delete_count=count)
        else:
            self.update_run(run_id, status="COMPLETED", total_emails=0)
        return count

    def resolve_all_needs_review(self, action: str, run_id: Optional[str] = None) -> int:
        """Bulk marks all emails currently needing review (final_action = 'REVIEW') to either 'KEEP' or 'DELETE' and stamps run_id."""
        action = action.strip().upper()
        if action not in ("KEEP", "DELETE"):
            raise ValueError("Action must be 'KEEP' or 'DELETE'")
        if not run_id:
            run_id = self.create_run(action_type=f"Batch Review ({action})")
        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE emails SET
                    final_action = ?,
                    is_reviewed = 1,
                    revalidation_status = ?,
                    revalidation_notes = 'Resolved via batch review in UI',
                    last_run_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND final_action = 'REVIEW' AND status != 'TRASHED'
            """, (action, f"BATCH_{action}", run_id, self.account))
            count = cursor.rowcount
            logger.info(f"Batch resolved {count} review items to {action} (Run ID: {run_id})")
        if count > 0:
            self.update_run(
                run_id,
                status="COMPLETED",
                total_emails=count,
                delete_count=count if action == "DELETE" else 0,
                keep_count=count if action == "KEEP" else 0,
            )
        else:
            self.update_run(run_id, status="COMPLETED", total_emails=0)
        return count

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
        Updates batch of emails with Stage 2 AI Classifier multi-status decisions and stamps last_run_id.
        Expected items: {"id" or "uid": <uid>, "status" or "ai_decision": <status>, "confidence": "...", "category": "...", "reason": "...", "last_run_id": "..."}
        """
        if not scan_results:
            return 0

        sql = """
            UPDATE emails SET
                ai_decision = :ai_decision,
                ai_confidence = :ai_confidence,
                ai_category = :ai_category,
                ai_reason = :ai_reason,
                final_action = :final_action,
                status = 'SCANNED',
                last_run_id = COALESCE(NULLIF(:last_run_id, ''), last_run_id),
                updated_at = CURRENT_TIMESTAMP
            WHERE account = :account AND uid = :uid
        """
        params = []
        for r in scan_results:
            uid_val = int(r.get("id") or r.get("uid"))
            status_val = (r.get("status") or r.get("ai_decision") or "NEEDS_REVIEW").strip().upper()
            
            # Map granular multi-status to initial action
            if status_val in ("CONFIDENT_DELETE", "PROBABLE_DELETE", "DELETE"):
                action = "DELETE"
            elif status_val in ("CONFIDENT_KEEP", "PROBABLE_KEEP", "KEEP"):
                action = "KEEP"
            elif status_val in ("NEEDS_REVIEW", "REVIEW"):
                action = "REVIEW"
            else:
                action = "REVIEW"

            params.append({
                "account": self.account,
                "uid": uid_val,
                "ai_decision": status_val,
                "ai_confidence": (r.get("confidence") or r.get("ai_confidence") or "MEDIUM").strip().upper(),
                "ai_category": (r.get("category") or r.get("ai_category") or "OTHER").strip().upper(),
                "ai_reason": r.get("reason") or r.get("ai_reason", "Unclassified"),
                "final_action": action,
                "last_run_id": r.get("last_run_id") or r.get("run_id") or "",
            })

        with self.get_connection() as conn:
            conn.executemany(sql, params)
        logger.debug(f"Updated scan batch of {len(params)} emails in DB")
        return len(params)

    def update_audit_batch(self, audit_results: List[Dict[str, Any]]) -> int:
        """
        Updates batch of emails with Stage 3 Safety Auditor multi-status decisions and stamps last_run_id.
        Expected items: {"id" or "uid": <uid>, "validator_status" or "validator_decision": <status>, "validator_confidence": "...", "validator_reason": "...", "last_run_id": "..."}
        """
        if not audit_results:
            return 0

        sql = """
            UPDATE emails SET
                validator_decision = :val_dec,
                validator_confidence = :val_conf,
                validator_reason = :val_rsn,
                revalidation_status = :val_dec,
                revalidation_notes = :val_rsn,
                final_action = :final_action,
                status = 'AUDITED',
                last_run_id = COALESCE(NULLIF(:last_run_id, ''), last_run_id),
                updated_at = CURRENT_TIMESTAMP
            WHERE account = :account AND uid = :uid
        """
        params = []
        for r in audit_results:
            uid_val = int(r.get("id") or r.get("uid"))
            val_dec = (r.get("validator_status") or r.get("validator_decision") or "CONFIRMED_DELETE").strip().upper()
            val_conf = (r.get("validator_confidence") or r.get("confidence") or "HIGH").strip().upper()
            val_rsn = r.get("validator_reason") or r.get("reason", "Audited")

            # Multi-status action mapping
            if val_dec in ("CONFIRMED_DELETE", "SOFT_DELETE"):
                action = "DELETE"
            elif val_dec in ("CONFIRMED_KEEP", "SUGGEST_KEEP", "OVERRIDE_KEEP"):
                action = "KEEP"
            elif val_dec in ("NEEDS_USER_REVIEW", "NEEDS_REVIEW", "REVIEW"):
                action = "REVIEW"
            else:
                action = "DELETE"

            params.append({
                "account": self.account,
                "uid": uid_val,
                "val_dec": val_dec,
                "val_conf": val_conf,
                "val_rsn": val_rsn,
                "final_action": action,
                "last_run_id": r.get("last_run_id") or r.get("run_id") or "",
            })

        with self.get_connection() as conn:
            conn.executemany(sql, params)
        logger.debug(f"Updated audit batch of {len(params)} emails in DB")
        return len(params)

    def mark_trashed(self, uids: List[Union[int, str]], run_id: Optional[str] = None) -> int:
        """Marks a list of UIDs as successfully moved to Gmail Trash and stamps last_run_id."""
        if not uids:
            return 0

        int_uids = [int(u) for u in uids]
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in int_uids)
            sql = f"""
                UPDATE emails SET
                    status = 'TRASHED',
                    trashed_at = CURRENT_TIMESTAMP,
                    last_run_id = COALESCE(?, last_run_id),
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid IN ({placeholders})
            """
            cursor = conn.execute(sql, (run_id, self.account, *int_uids))
            count = cursor.rowcount
            logger.info(f"Marked {count} emails as TRASHED in DB (Run ID: {run_id or 'none'})")
            return count

    def mark_restored(self, uids: List[Union[int, str]], run_id: Optional[str] = None) -> int:
        """Marks a list of UIDs as restored back to Inbox and stamps last_run_id."""
        if not uids:
            return 0

        int_uids = [int(u) for u in uids]
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in int_uids)
            sql = f"""
                UPDATE emails SET
                    status = 'RESTORED',
                    trashed_at = NULL,
                    last_run_id = COALESCE(?, last_run_id),
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid IN ({placeholders})
            """
            cursor = conn.execute(sql, (run_id, self.account, *int_uids))
            count = cursor.rowcount
            logger.info(f"Marked {count} emails as RESTORED in DB (Run ID: {run_id or 'none'})")
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

    def set_manual_override(self, uid: Union[int, str], action: str, note: str = "Manual UI override", run_id: Optional[str] = None) -> bool:
        """Manually flips an email's final action between KEEP, DELETE, or REVIEW, optionally stamping run_id."""
        action = action.strip().upper()
        if action not in ("KEEP", "DELETE", "REVIEW"):
            raise ValueError(f"Action must be 'KEEP', 'DELETE', or 'REVIEW', got '{action}'")

        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE emails SET
                    final_action = ?,
                    is_reviewed = 1,
                    revalidation_status = ?,
                    revalidation_notes = ?,
                    last_run_id = COALESCE(?, last_run_id),
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid = ?
            """, (action, f"MANUAL_{action}", note, run_id, self.account, int(uid)))
            affected = cursor.rowcount > 0
            if affected:
                logger.info(f"Manual override applied: UID {uid} -> {action} ({note})")
            return affected

    def bulk_set_final_action(self, uids: List[Union[int, str]], action: str, note: str = "Browser review decision", run_id: Optional[str] = None) -> int:
        """Sets final_action (KEEP or DELETE) for a batch of UIDs from the browser review UI and stamps run_id."""
        if not uids:
            return 0
        action = action.strip().upper()
        if action not in ("KEEP", "DELETE", "REVIEW"):
            raise ValueError(f"Invalid action: {action}")
        int_uids = [int(u) for u in uids]
        with self.get_connection() as conn:
            placeholders = ",".join("?" for _ in int_uids)
            sql = f"""
                UPDATE emails SET
                    final_action = ?,
                    is_reviewed = 1,
                    revalidation_notes = ?,
                    last_run_id = COALESCE(?, last_run_id),
                    updated_at = CURRENT_TIMESTAMP
                WHERE account = ? AND uid IN ({placeholders})
            """
            cursor = conn.execute(sql, (action, note, run_id, self.account, *int_uids))
            count = cursor.rowcount
            logger.info(f"Bulk override: set {count} emails to {action} (Run ID: {run_id or 'none'})")
            return count

    def get_emails_needing_review(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetches all emails that require human review in the browser."""
        with self.get_connection() as conn:
            sql = """
                SELECT * FROM emails 
                WHERE account = ? AND final_action = 'REVIEW' AND status != 'TRASHED'
                ORDER BY uid DESC
            """
            if limit:
                sql += f" LIMIT {int(limit)}"
            cursor = conn.execute(sql, (self.account,))
            return [dict(row) for row in cursor.fetchall()]

    def get_emails_page(
        self,
        search: str = "",
        action_filter: str = "ALL",
        status_filter: str = "ALL",
        ai_decision_filter: str = "ALL",
        category_filter: str = "ALL",
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Fast paginated query with multi-field search and multi-status filters for UI explorer.
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

        if ai_decision_filter and ai_decision_filter.upper() != "ALL":
            conditions.append("ai_decision = ?")
            params.append(ai_decision_filter.upper())

        if category_filter and category_filter.upper() != "ALL":
            conditions.append("ai_category = ?")
            params.append(category_filter.upper())

        if search and search.strip():
            term = f"%{search.strip()}%"
            conditions.append("(sender LIKE ? OR subject LIKE ? OR snippet LIKE ? OR CAST(uid AS TEXT) LIKE ? OR last_run_id LIKE ? OR ai_reason LIKE ? OR validator_reason LIKE ?)")
            params.extend([term, term, term, term, term, term, term])

        where_clause = " AND ".join(conditions)

        with self.get_connection() as conn:
            # Count query
            count_sql = f"SELECT COUNT(*) FROM emails WHERE {where_clause}"
            total_count = conn.execute(count_sql, tuple(params)).fetchone()[0]

            # Data query
            data_sql = f"""
                SELECT uid, date, sender, subject, snippet, is_starred, is_reply,
                       status, ai_decision, ai_confidence, ai_category, ai_reason,
                       validator_decision, validator_confidence, validator_reason,
                       final_action, is_reviewed, revalidation_status, revalidation_notes,
                       last_run_id, updated_at
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
            ON CONFLICT(run_id) DO UPDATE SET
                action_type = excluded.action_type,
                params_json = CASE WHEN excluded.params_json != '{}' THEN excluded.params_json ELSE runs.params_json END,
                notes = CASE WHEN excluded.notes != '' THEN excluded.notes ELSE runs.notes END
        """
        with self.get_connection() as conn:
            conn.execute(sql, (run_id, self.account, action_type, params_json, notes))
        logger.info(f"Created or synced run record '{run_id}' for action '{action_type}'")
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

