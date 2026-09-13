"""Artifact path management, account directory isolation, and cursor persistence."""

from datetime import datetime
import glob
import json
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


def get_state_file(email_addr=None):
    """Returns path to state.json for an email account."""
    return os.path.join(get_account_dir(email_addr), "state.json")


def generate_artifact_path(step_name, prefix, email_addr=None):
    """Generates unique timestamped artifact path: outputs/<email>/<step_name>/<prefix>_<timestamp>.csv."""
    step_dir = get_step_dir(step_name, email_addr)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(step_dir, f"{prefix}_{timestamp}.csv")


def get_latest_artifact(step_name, email_addr=None, pattern="*.csv"):
    """Finds the most recent artifact file in a step's directory."""
    step_dir = get_step_dir(step_name, email_addr)
    files = glob.glob(os.path.join(step_dir, pattern))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def load_state(email_addr=None):
    """Loads cursor and run history for a specific email account."""
    state_file = get_state_file(email_addr)
    if os.path.exists(state_file):
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not read {state_file} ({e}), initializing fresh state.")
    return {
        "account": email_addr or GMAIL_USER,
        "uid_validity": None,
        "last_processed_uid": 0,
        "total_scanned": 0,
        "last_run_at": None,
    }


def save_state(state, email_addr=None):
    """Saves updated state for a specific email account."""
    state_file = get_state_file(email_addr)
    try:
        with open(state_file, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"⚠️ Warning: Could not save state to {state_file}: {e}")
