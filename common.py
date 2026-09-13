import csv
from datetime import datetime
import email
from email.header import decode_header
import glob
from html.parser import HTMLParser
import imaplib
import json
import os
import re
import sys
import time
import warnings
from google import genai
from google.genai import types
import dotenv

# Suppress Google GenAI SDK automatic function calling advisory warning
warnings.filterwarnings("ignore", message=".*Direct use of automatic function calling.*")

# ==================== ENVIRONMENT & DEFAULTS ====================
dotenv.load_dotenv()
GMAIL_USER = os.getenv("GMAIL_USER", "your-email@gmail.com")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "your-16-char-app-password")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "your-gemini-api-key")

IMAP_SERVER = "imap.gmail.com"
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

DEFAULT_BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DEFAULT_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
DEFAULT_SNIPPET_LENGTH = int(os.getenv("SNIPPET_LENGTH", "250"))
OUTPUTS_ROOT = "outputs"
# ================================================================


# ==================== ACCOUNT & DIRECTORY HELPERS ====================
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
# ====================================================================


# ==================== STATE MANAGEMENT ====================
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
# ==========================================================


# ==================== CREDENTIAL VALIDATION ====================
def validate_imap_credentials(email_user=None, app_password=None):
    """Validates that IMAP credentials are not missing or default placeholders."""
    user = (email_user or GMAIL_USER or "").strip()
    pwd = (app_password or GMAIL_APP_PASSWORD or "").strip()

    placeholders = {"your-email@gmail.com", "your_email@gmail.com", ""}
    pwd_placeholders = {"your-16-char-app-password", "xxxx-xxxx-xxxx-xxxx", ""}

    if not user or user in placeholders:
        print("\n❌ Configuration Error: GMAIL_USER is not set in your .env file.")
        print("   Please copy .env.example to .env and set your Gmail address.")
        sys.exit(1)

    if not pwd or pwd in pwd_placeholders:
        print("\n❌ Configuration Error: GMAIL_APP_PASSWORD is not set in your .env file.")
        print("   Please generate a 16-character Google App Password (2-Step Verification required):")
        print("   https://myaccount.google.com/apppasswords")
        print("   Then set GMAIL_APP_PASSWORD in your .env file.")
        sys.exit(1)

    return user, pwd


def validate_gemini_credentials(api_key=None):
    """Validates that Gemini API key is configured."""
    key = (api_key or GEMINI_API_KEY or "").strip()
    placeholders = {"your-gemini-api-key", "AIzaSy...", ""}
    if not key or key in placeholders:
        print("\n❌ Configuration Error: GEMINI_API_KEY is not set in your .env file.")
        print("   Please get an API key from Google AI Studio:")
        print("   https://aistudio.google.com/app/apikey")
        print("   Then set GEMINI_API_KEY in your .env file.")
        sys.exit(1)
    return key
# ===============================================================


# ==================== IMAP CONNECTION ====================
def connect_imap(email_user=None, app_password=None):
    """Establishes a single dedicated SSL connection to Gmail IMAP."""
    user, pwd = validate_imap_credentials(email_user, app_password)
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(user, pwd)
        return mail
    except imaplib.IMAP4.error as e:
        err_msg = str(e)
        if "AUTHENTICATIONFAILED" in err_msg:
            print(f"\n❌ IMAP Authentication Failed for '{user}'.")
            print("   Common causes:")
            print("   1. You must use a 16-character Google App Password (NOT your regular account password).")
            print("   2. 2-Step Verification must be enabled: https://myaccount.google.com/apppasswords")
            print("   3. IMAP must be enabled in Gmail Settings -> Forwarding and POP/IMAP.")
        else:
            print(f"❌ IMAP Connection Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected IMAP Error: {e}")
        raise
# =========================================================


# ==================== EMAIL PARSING & HTML STRIPPER ====================
class SimpleHTMLTextExtractor(HTMLParser):
    """Extracts clean plain text from HTML sections."""
    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.skip_tags = {"script", "style", "head", "title", "meta", "noscript"}
        self.current_tag = None

    def handle_starttag(self, tag, attrs):
        self.current_tag = tag.lower()

    def handle_endtag(self, tag):
        self.current_tag = None

    def handle_data(self, data):
        if self.current_tag not in self.skip_tags:
            cleaned = data.strip()
            if cleaned:
                self.text_parts.append(cleaned)

    def get_text(self):
        return " ".join(self.text_parts)


def get_decoded_header(msg, header_name):
    """Safely decodes email header fields handling unknown-8bit and diverse character encodings."""
    value = msg.get(header_name, "")
    if not value:
        return ""
    try:
        decoded_parts = decode_header(value)
    except Exception:
        return str(value)

    header_str = ""
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            decoded_text = None
            if encoding:
                try:
                    decoded_text = part.decode(encoding, errors="ignore")
                except (LookupError, UnicodeDecodeError, ValueError):
                    decoded_text = None
            if decoded_text is None:
                try:
                    decoded_text = part.decode("utf-8", errors="ignore")
                except Exception:
                    decoded_text = part.decode("latin-1", errors="ignore")
            header_str += decoded_text
        else:
            header_str += str(part)
    return header_str.strip()


def extract_body_snippet(msg, max_chars=DEFAULT_SNIPPET_LENGTH):
    """Extracts plain-text snippet, falling back to HTML-stripped text if text/plain is absent."""
    plain_text = ""
    html_text = ""

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition"))
            if "attachment" in content_disposition:
                continue

            if content_type == "text/plain" and not plain_text:
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        plain_text = payload.decode("utf-8", errors="ignore")
                except Exception:
                    pass
            elif content_type == "text/html" and not html_text:
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        html_raw = payload.decode("utf-8", errors="ignore")
                        parser = SimpleHTMLTextExtractor()
                        parser.feed(html_raw)
                        html_text = parser.get_text()
                except Exception:
                    pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                raw_text = payload.decode("utf-8", errors="ignore")
                if msg.get_content_type() == "text/html":
                    parser = SimpleHTMLTextExtractor()
                    parser.feed(raw_text)
                    html_text = parser.get_text()
                else:
                    plain_text = raw_text
        except Exception:
            pass

    final_text = plain_text if plain_text.strip() else html_text
    clean_text = " ".join(final_text.split())
    return clean_text[:max_chars]
# =======================================================================


# ==================== JSON PARSER & REPAIR ====================
def safe_parse_json_array(raw_text):
    """Safely parses JSON array, automatically repairing truncated output if string was cut off."""
    if not raw_text:
        return []
    text = raw_text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        last_brace = text.rfind("}")
        if last_brace != -1:
            repaired = text[:last_brace + 1] + "\n]"
            try:
                return json.loads(repaired)
            except Exception:
                pass
    return []
# ===============================================================
