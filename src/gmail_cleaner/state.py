"""Artifact path management, account directory isolation, and cursor persistence."""

from datetime import datetime
import glob
import os
from gmail_cleaner.config import OUTPUTS_ROOT, GMAIL_USER


def sanitize_account_name(email_addr=None):
    """Returns a clean folder-safe string for an email address."""
    addr = (email_addr or GMAIL_USER).strip().lower()
    return addr.replace("@", "_at_").replace(".", "_")


def get_account_dir(email_addr=None):
    """Returns base directory for a specific email account: outputs/<email>/."""
    account_name = sanitize_account_name(email_addr)
    acc_dir = os.path.join(OUTPUTS_ROOT, account_name)
    os.makedirs(acc_dir, exist_ok=True)
    return acc_dir


def get_step_dir(step_name, email_addr=None):
    """Returns step directory: outputs/<email>/<step_name>/."""
    step_dir = os.path.join(get_account_dir(email_addr), step_name)
    os.makedirs(step_dir, exist_ok=True)
    return step_dir


def generate_artifact_path(step_name, prefix, email_addr=None):
    """Generates unique timestamped artifact path: outputs/<email>/<step_name>/<prefix>_<timestamp>.csv."""
    step_dir = get_step_dir(step_name, email_addr)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(step_dir, f"{prefix}_{timestamp}.csv")


def get_latest_artifact(step_name, email_addr=None, pattern="*.csv"):
    """Finds the most recent artifact file in a step's directory."""
    step_dir = get_step_dir(step_name, email_addr)
    if not os.path.exists(step_dir):
        return None
    files = glob.glob(os.path.join(step_dir, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def load_state(email_addr=None):
    """Loads cursor and run history directly from SQLite DB (accounts table)."""
    from gmail_cleaner.db import EmailDB
    account = (email_addr or GMAIL_USER).strip().lower()
    db = EmailDB(account=account)
    acc = db.get_account(account)
    if acc:
        return {
            "account": account,
            "uid_validity": acc.get("uid_validity"),
            "last_processed_uid": acc.get("last_uid_scanned", 0),
            "total_scanned": acc.get("total_scanned", 0),
            "last_run_at": acc.get("last_fetched_at"),
        }
    return {
        "account": account,
        "uid_validity": None,
        "last_processed_uid": 0,
        "total_scanned": 0,
        "last_run_at": None,
    }


def save_state(state, email_addr=None):
    """Saves updated state directly to SQLite DB (accounts table)."""
    from gmail_cleaner.db import EmailDB
    account = (email_addr or state.get("account") or GMAIL_USER).strip().lower()
    db = EmailDB(account=account)
    db.update_account_cursor(
        account=account,
        last_uid=state.get("last_processed_uid", 0),
        total_scanned=state.get("total_scanned", 0),
        uid_validity=state.get("uid_validity")
    )
