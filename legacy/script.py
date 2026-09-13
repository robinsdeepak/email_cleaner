# ==============================================================================
# NOTE: This is the legacy monolithic version of the email cleaner.
# It is kept here for historical reference only.
# Please use the modular pipeline via `pipeline.py` or `Makefile` instead.
# ==============================================================================

import argparse
import concurrent.futures
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
import shutil
import sys
import time
import warnings
from google import genai
from google.genai import types
import dotenv

# Suppress Google GenAI SDK automatic function calling advisory warning
warnings.filterwarnings("ignore", message=".*Direct use of automatic function calling.*")

# ==================== CONFIGURATION ====================
dotenv.load_dotenv()
GMAIL_USER = os.getenv("GMAIL_USER", "your-email@gmail.com")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "your-16-char-app-password")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "your-gemini-api-key")

IMAP_SERVER = "imap.gmail.com"
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Performance & Rate Limit Parameters (Configurable via ENV or CLI)
DEFAULT_BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))       # 50 emails per prompt for maximum throughput
DEFAULT_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))     # 10 concurrent IMAP/API worker threads (Gmail max ~15)
DEFAULT_SNIPPET_LENGTH = int(os.getenv("SNIPPET_LENGTH", "250"))  # Max chars of plain text body snippet

# Directory and Artifact Structure
OUTPUT_DIR = "outputs"
PENDING_DIR = os.path.join(OUTPUT_DIR, "pending_review")
PROCESSED_DIR = os.path.join(OUTPUT_DIR, "processed")
STATE_FILE = os.path.join(OUTPUT_DIR, "state.json")
# =======================================================


def ensure_output_directories():
    """Ensures required artifact directories exist."""
    os.makedirs(PENDING_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def get_latest_pending_file():
    """Returns the most recent review file in outputs/pending_review/."""
    ensure_output_directories()
    files = glob.glob(os.path.join(PENDING_DIR, "*.csv"))
    if not files:
        if os.path.exists("pending_review.csv"):
            return "pending_review.csv"
        return None
    return max(files, key=os.path.getmtime)


def get_latest_processed_file():
    """Returns the most recent completed file in outputs/processed/."""
    ensure_output_directories()
    files = glob.glob(os.path.join(PROCESSED_DIR, "completed_*.csv"))
    if not files:
        # Fallback to any csv in processed
        all_csvs = glob.glob(os.path.join(PROCESSED_DIR, "*.csv"))
        return max(all_csvs, key=os.path.getmtime) if all_csvs else None
    return max(files, key=os.path.getmtime)


# ==================== STATE MANAGEMENT ====================
def load_state():
    """Loads cursor and run history from outputs/state.json (with fallback to legacy state.json)."""
    ensure_output_directories()
    target_path = STATE_FILE if os.path.exists(STATE_FILE) else ("state.json" if os.path.exists("state.json") else None)

    if target_path:
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not read {target_path} ({e}), initializing fresh state.")
    return {
        "uid_validity": None,
        "last_processed_uid": 0,
        "total_scanned": 0,
        "last_run_at": None,
    }


def save_state(state):
    """Saves updated state back to outputs/state.json."""
    ensure_output_directories()
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"⚠️ Warning: Could not save state to {STATE_FILE}: {e}")


def get_uid_validity(mail):
    """Retrieves UIDVALIDITY of INBOX to ensure cursor continuity."""
    status, data = mail.status("INBOX", "(UIDVALIDITY)")
    if status == "OK" and data and data[0]:
        match = re.search(r"UIDVALIDITY\s+(\d+)", data[0].decode("utf-8", errors="ignore"))
        if match:
            return match.group(1)
    return None
# ==========================================================


# ==================== EMAIL PARSING & HTML STRIPPER ====================
class SimpleHTMLTextExtractor(HTMLParser):
    """Lightweight HTML-to-text converter to extract clean text from HTML-only emails."""
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
    """Safely decodes email header fields handling diverse and malformed character encodings."""
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
            # 1. Try specified encoding if valid
            if encoding:
                try:
                    decoded_text = part.decode(encoding, errors="ignore")
                except (LookupError, UnicodeDecodeError, ValueError):
                    decoded_text = None
            # 2. Fall back to UTF-8
            if decoded_text is None:
                try:
                    decoded_text = part.decode("utf-8", errors="ignore")
                except Exception:
                    # 3. Fall back to Latin-1 (never fails on any 8-bit bytes)
                    decoded_text = part.decode("latin-1", errors="ignore")
            header_str += decoded_text
        else:
            header_str += str(part)
    return header_str.strip()


def extract_body_snippet(msg, max_chars=DEFAULT_SNIPPET_LENGTH):
    """
    Extracts a short plain-text snippet. If plain-text is missing (HTML-only email),
    falls back to stripping tags from text/html so the snippet is never blank.
    """
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

    # Use plain text if available; otherwise fall back to converted HTML text
    final_text = plain_text if plain_text.strip() else html_text
    clean_text = " ".join(final_text.split())
    return clean_text[:max_chars]
# =======================================================


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
        # If output was truncated at token limit, rescue all complete objects up to the last '}'
        last_brace = text.rfind("}")
        if last_brace != -1:
            repaired = text[:last_brace + 1] + "\n]"
            try:
                data = json.loads(repaired)
                return data
            except Exception:
                pass
    return []


# ==================== GEMINI CLASSIFICATION ====================
def classify_batch_with_gemini(client, batch_payload, max_retries=3):
    """Sends a batch of emails to Gemini and returns structured decisions with exponential backoff."""
    prompt = f"""
Analyze this batch of emails and determine whether each is safe to delete or should be kept.

STRICT RULES:
- DELETE (True): Promotional offers, marketing newsletters, discounts, automated sales campaigns, cold outreach, spam, unsolicited announcements.
- KEEP (False): Receipts, invoices, order confirmations, shipping updates, travel/hotel bookings, tickets, legal/tax compliance notices, personal messages, official bank/financial notifications.

FORMAT:
- Keep 'reason' extremely brief (under 10 words).

Emails:
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
                                "delete": {"type": "BOOLEAN"},
                                "reason": {"type": "STRING"}
                            },
                            "required": ["id", "delete", "reason"]
                        }
                    }
                )
            )
            parsed = safe_parse_json_array(response.text)
            if parsed:
                return parsed
            print(f"⚠️ Warning: Could not parse response as JSON array on attempt {attempt + 1}")
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                print(f"   ⏳ Throttled (429). Retrying batch in {backoff_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    print(f"⚠️ API Error processing batch after {max_retries} retries: {e}")
                else:
                    print(f"⚠️ API Error processing batch with Gemini: {e}")
                return []
    return []


def audit_batch_with_gemini(client, batch_payload, max_retries=3):
    """Second-pass LLM auditor: specifically scrutinizes candidate DELETIONS for false positives."""
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
            print(f"⚠️ Warning: Auditor response could not be parsed as JSON array on attempt {attempt + 1}")
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
# ===============================================================


# ==================== STEP 1: SCAN & EXPORT ====================
def fetch_and_evaluate_batch(uids_chunk, mail_connection_info, ai_client, snippet_length):
    """Worker task: Fetches email metadata via IMAP UIDs, protects Starred/Replies, then classifies with Gemini."""
    user, pwd, server = mail_connection_info
    try:
        mail = imaplib.IMAP4_SSL(server)
        mail.login(user, pwd)
        mail.select("INBOX")
    except Exception as e:
        print(f"⚠️ Worker IMAP connection error: {e}")
        return []

    batch_payload = []
    metadata_map = {}
    pre_protected_results = []

    for uid in uids_chunk:
        uid_bytes = str(uid).encode("utf-8")
        # Fetch FLAGS along with message body to check for Starred status
        status, msg_data = mail.uid("fetch", uid_bytes, "(FLAGS BODY.PEEK[])")
        if status != "OK" or not msg_data:
            continue

        raw_flags = b""
        msg = None

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                raw_flags = response_part[0]
                try:
                    msg = email.message_from_bytes(response_part[1])
                except Exception as parse_err:
                    print(f"⚠️ Error parsing message UID {uid}: {parse_err}")

        if not msg:
            continue

        try:
            sender = get_decoded_header(msg, "From")
            subject = get_decoded_header(msg, "Subject")
            date_str = get_decoded_header(msg, "Date")
            snippet = extract_body_snippet(msg, max_chars=snippet_length)
        except Exception as extract_err:
            print(f"⚠️ Warning: Could not parse email UID {uid}: {extract_err}")
            continue
        uid_str = str(uid)

        # Rule 1: Auto-protect Starred emails (\Flagged in IMAP)
        is_starred = b"\\Flagged" in raw_flags
        if is_starred:
            pre_protected_results.append({
                "uid": uid_str,
                "date": date_str,
                "from": sender,
                "subject": subject,
                "snippet": snippet,
                "ai_decision": "KEEP",
                "ai_reason": "⭐ Starred by user in Gmail",
                "validator_decision": "N/A",
                "validator_reason": "Protected by Starred status",
                "final_action": "KEEP"
            })
            continue

        # Rule 2: Auto-protect replies / active conversation threads
        has_reply_header = bool(msg.get("In-Reply-To") or msg.get("References"))
        if has_reply_header:
            pre_protected_results.append({
                "uid": uid_str,
                "date": date_str,
                "from": sender,
                "subject": subject,
                "snippet": snippet,
                "ai_decision": "KEEP",
                "ai_reason": "💬 Active conversation thread / reply",
                "validator_decision": "N/A",
                "validator_reason": "Protected thread reply",
                "final_action": "KEEP"
            })
            continue

        # Otherwise, queue for Gemini classification
        metadata_map[uid_str] = {
            "uid": uid_str,
            "date": date_str,
            "from": sender,
            "subject": subject,
            "snippet": snippet,
        }
        batch_payload.append({
            "id": uid_str,
            "from": sender,
            "subject": subject,
            "snippet": snippet
        })

    mail.close()
    mail.logout()

    results = list(pre_protected_results)

    if batch_payload:
        evaluations = classify_batch_with_gemini(ai_client, batch_payload)
        eval_by_id = {item.get("id"): item for item in evaluations if item.get("id")}

        for uid_str, meta in metadata_map.items():
            eval_item = eval_by_id.get(uid_str, {})
            should_delete = eval_item.get("delete", False)
            decision = "DELETE" if should_delete else "KEEP"
            reason = eval_item.get("reason", "Unclassified or API error")

            results.append({
                "uid": uid_str,
                "date": meta["date"],
                "from": meta["from"],
                "subject": meta["subject"],
                "snippet": meta["snippet"],
                "ai_decision": decision,
                "ai_reason": reason,
                "validator_decision": "PENDING_VALIDATION" if decision == "DELETE" else "N/A",
                "validator_reason": "Not yet audited" if decision == "DELETE" else "Retained by initial scan",
                "final_action": decision
            })

    return results


def run_scan(limit=100, direction="oldest-first", output_file=None, reset_cursor=False,
             batch_size=DEFAULT_BATCH_SIZE, workers=DEFAULT_MAX_WORKERS, snippet_length=DEFAULT_SNIPPET_LENGTH,
             auto_validate=False):
    """Scans emails from Gmail, evaluates them with Gemini, and writes a uniquely named review CSV."""
    ensure_output_directories()

    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(PENDING_DIR, f"pending_review_{timestamp}.csv")

    print(f"\n🚀 [STAGE 1: HIGH-SPEED SCAN & TRIAGE]")
    print(f"   • Limit         : {limit} emails")
    print(f"   • Order         : {direction}")
    print(f"   • Concurrency   : {workers} workers")
    print(f"   • Batch Size    : {batch_size} emails/request")
    print(f"   • Snippet Length: {snippet_length} chars")
    print(f"   • Output File   : {output_file}")

    state = load_state()
    if reset_cursor:
        print("🔄 Resetting cursor to start from the beginning.")
        state["last_processed_uid"] = 0

    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
    mail.select("INBOX")

    current_validity = get_uid_validity(mail)
    if state["uid_validity"] and current_validity and state["uid_validity"] != current_validity:
        print("⚠️ Mailbox UIDVALIDITY changed. Resetting cursor for safety.")
        state["last_processed_uid"] = 0
    state["uid_validity"] = current_validity

    last_uid = state.get("last_processed_uid", 0)
    print(f"   • Cursor State  : last_processed_uid = {last_uid}")

    if direction == "oldest-first" and last_uid > 0:
        search_criteria = f"UID {last_uid + 1}:*"
        status, messages = mail.uid("search", None, search_criteria)
    else:
        status, messages = mail.uid("search", None, "ALL")

    if status != "OK" or not messages[0]:
        print("✅ No emails found matching search criteria.")
        mail.close()
        mail.logout()
        return

    raw_uids = [int(x) for x in messages[0].split()]
    if direction == "oldest-first" and last_uid > 0:
        raw_uids = [uid for uid in raw_uids if uid > last_uid]

    if not raw_uids:
        print("✅ No new emails to process since last cursor.")
        mail.close()
        mail.logout()
        return

    if direction == "oldest-first":
        raw_uids.sort()
    else:
        raw_uids.sort(reverse=True)

    selected_uids = raw_uids[:limit]
    print(f"Discovered {len(raw_uids)} candidate emails. Processing next {len(selected_uids)} emails...")

    mail.close()
    mail.logout()

    chunks = [selected_uids[i:i + batch_size] for i in range(0, len(selected_uids), batch_size)]
    print(f"Divided into {len(chunks)} batch(es) across {min(workers, len(chunks))} active worker threads.")

    ai_client = genai.Client(api_key=GEMINI_API_KEY)
    mail_info = (GMAIL_USER, GMAIL_APP_PASSWORD, IMAP_SERVER)

    all_evaluated = []
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(fetch_and_evaluate_batch, chunk, mail_info, ai_client, snippet_length)
            for chunk in chunks
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                all_evaluated.extend(res)
            except Exception as exc:
                print(f"⚠️ Batch generated an exception: {exc}")

    if not all_evaluated:
        print("⚠️ No email evaluations returned.")
        return

    uid_order = {uid: idx for idx, uid in enumerate(selected_uids)}
    all_evaluated.sort(key=lambda x: uid_order.get(int(x["uid"]), 0))

    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)

    fieldnames = [
        "uid", "date", "from", "subject", "snippet",
        "ai_decision", "ai_reason", "validator_decision", "validator_reason", "final_action"
    ]
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(all_evaluated)

    max_uid_processed = max(int(item["uid"]) for item in all_evaluated)
    if direction == "oldest-first":
        state["last_processed_uid"] = max(state.get("last_processed_uid", 0), max_uid_processed)
    state["total_scanned"] = state.get("total_scanned", 0) + len(all_evaluated)
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state)

    elapsed = time.time() - start_time
    emails_per_sec = len(all_evaluated) / elapsed if elapsed > 0 else 0
    delete_count = sum(1 for x in all_evaluated if x["ai_decision"] == "DELETE")
    keep_count = len(all_evaluated) - delete_count

    print(f"\n📊 [SCAN COMPLETE] in {elapsed:.2f}s ({emails_per_sec:.1f} emails/second)")
    print(f"   • Processed: {len(all_evaluated)} emails")
    print(f"   • Recommended DELETE: {delete_count}")
    print(f"   • Recommended KEEP:   {keep_count}")
    print(f"   • New cursor: last_processed_uid = {state['last_processed_uid']} (saved in {STATE_FILE})")
    print(f"\n📁 Review spreadsheet exported to: {output_file}")

    if auto_validate and delete_count > 0:
        print("\n🔍 Running automatic second-pass validation...")
        run_validate(review_file=output_file, workers=workers, batch_size=batch_size)
    else:
        print(f"👉 Next Step (Optional): Run LLM Auditor validation:")
        print(f"   python script.py validate --file {output_file}")
        print(f"👉 When ready to execute, run:")
        print(f"   python script.py apply --file {output_file}\n")


# ==================== STEP 2: VALIDATE (LLM AUDITOR) ====================
def run_validate(review_file=None, workers=DEFAULT_MAX_WORKERS, batch_size=DEFAULT_BATCH_SIZE):
    """
    Second-pass LLM validation: Reads a pending review CSV, audits all items marked DELETE,
    rescues any false positives by changing final_action to KEEP, and writes the audited CSV.
    """
    ensure_output_directories()

    if not review_file:
        review_file = get_latest_pending_file()

    print(f"\n🔬 [STAGE 2: LLM FALSE-POSITIVE AUDIT]")
    print(f"   • Target Review File: {review_file}")

    if not review_file or not os.path.exists(review_file):
        print(f"❌ Error: Review file '{review_file}' not found. Please run 'scan' first.")
        return

    rows = []
    with open(review_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        for row in reader:
            rows.append(row)

    if not rows:
        print("⚠️ Review file is empty.")
        return

    for col in ["validator_decision", "validator_reason"]:
        if col not in fieldnames:
            fieldnames.insert(fieldnames.index("final_action") if "final_action" in fieldnames else len(fieldnames), col)

    candidates_to_audit = [r for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    print(f"   • Total rows in review        : {len(rows)}")
    print(f"   • Candidate DELETES to audit  : {len(candidates_to_audit)}")

    if not candidates_to_audit:
        print("✅ No emails currently marked for deletion. Everything is set to KEEP!")
        return

    audit_chunks = [candidates_to_audit[i:i + batch_size] for i in range(0, len(candidates_to_audit), batch_size)]
    ai_client = genai.Client(api_key=GEMINI_API_KEY)
    
    start_time = time.time()
    audit_results = {}

    def audit_worker(chunk):
        payload = [
            {
                "id": r["uid"],
                "from": r.get("from", ""),
                "subject": r.get("subject", ""),
                "snippet": r.get("snippet", ""),
                "ai_reason": r.get("ai_reason", "")
            }
            for r in chunk
        ]
        return audit_batch_with_gemini(ai_client, payload)

    print(f"Dispatching {len(audit_chunks)} audit batch(es) to Gemini Safety Auditor...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(audit_worker, chunk) for chunk in audit_chunks]
        for f in concurrent.futures.as_completed(futures):
            try:
                res = f.result()
                for item in res:
                    if item.get("id"):
                        audit_results[item["id"]] = item
            except Exception as e:
                print(f"⚠️ Audit worker encountered an exception: {e}")

    rescued_count = 0
    confirmed_delete_count = 0

    for r in rows:
        uid = r.get("uid")
        if uid in audit_results:
            audit_item = audit_results[uid]
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
            if r.get("validator_decision") in [None, "", "PENDING_VALIDATION"]:
                r["validator_decision"] = "N/A"
                r["validator_reason"] = r.get("ai_reason", "Retained by initial scan")

    with open(review_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    elapsed = time.time() - start_time
    print(f"\n📊 [AUDIT COMPLETE] in {elapsed:.2f}s")
    print(f"   • Confirmed safe to delete: {confirmed_delete_count}")
    print(f"   • Rescued false positives : {rescued_count} (automatically switched to KEEP)")
    print(f"   • Updated file            : {review_file}")
    print(f"\n👉 Open '{review_file}' for final human inspection if desired.")
    print(f"👉 When ready to execute, run: python script.py apply --file {review_file}\n")


# ==================== STEP 3: APPLY REVIEW ====================
def run_apply(review_file=None, dry_run=False):
    """Reads human-reviewed CSV and moves confirmed 'DELETE' emails to Gmail Trash."""
    ensure_output_directories()

    if not review_file:
        review_file = get_latest_pending_file()

    print(f"\n🛡️ [STAGE 3: HUMAN REVIEW EXECUTION]")
    print(f"   • Target Review File: {review_file}")
    print(f"   • Dry Run Mode      : {dry_run}")

    if not review_file or not os.path.exists(review_file):
        print(f"❌ Error: Review file '{review_file}' not found.")
        print(f"   Please run 'scan' first or pass a file with --file.")
        return

    rows = []
    with open(review_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        print("⚠️ Review file is empty.")
        return

    to_delete = []
    to_keep = []

    for row in rows:
        action = (row.get("final_action") or "").strip().upper()
        if action == "DELETE":
            to_delete.append(row)
        else:
            to_keep.append(row)

    print(f"   • Total rows in review: {len(rows)}")
    print(f"   • Confirmed for DELETE: {len(to_delete)}")
    print(f"   • Confirmed to KEEP   : {len(to_keep)}")

    if not to_delete:
        print("✅ No emails marked with final_action = 'DELETE'. Nothing to do.")
        return

    if dry_run:
        print("\n🔍 [DRY RUN PREVIEW] The following emails WOULD be moved to Trash:")
        for r in to_delete[:20]:
            print(f"   ❌ UID {r['uid']} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        if len(to_delete) > 20:
            print(f"   ... and {len(to_delete) - 20} more.")
        print("\nNo changes made in Gmail. Run without '--dry-run' to execute deletion.")
        return

    print("\nConnecting to Gmail to move confirmed emails to Trash...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)
    mail.select("INBOX")

    uids_to_trash = [r["uid"] for r in to_delete]
    TRASH_BATCH_SIZE = 50
    success_count = 0

    for i in range(0, len(uids_to_trash), TRASH_BATCH_SIZE):
        batch = uids_to_trash[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        status, response = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Trash")
        if status == "OK":
            success_count += len(batch)
        else:
            print(f"⚠️ Warning: Could not trash UID batch: {uid_set}")

    mail.expunge()
    mail.close()
    mail.logout()

    print(f"🎉 Successfully moved {success_count} emails to Gmail Trash!")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = os.path.join(PROCESSED_DIR, f"completed_{timestamp}_{os.path.basename(review_file)}")
    try:
        shutil.move(review_file, archive_name)
        print(f"📦 Archived review file to: {archive_name}")
    except Exception as e:
        print(f"⚠️ Notice: Could not archive review file: {e}")
# ===============================================================


# ==================== STEP 4: UNDO / RESTORE ====================
def run_restore(archive_file=None, dry_run=False):
    r"""
    Reverses an email deletion by removing the \Trash label from Gmail
    and adding \Inbox, returning them safely back to your Inbox.
    """
    ensure_output_directories()

    if not archive_file:
        archive_file = get_latest_processed_file()

    print(f"\n🔄 [RESTORE / UNDO DELETION]")
    print(f"   • Target Archive File: {archive_file}")
    print(f"   • Dry Run Mode       : {dry_run}")

    if not archive_file or not os.path.exists(archive_file):
        print(f"❌ Error: No processed file found to restore in '{PROCESSED_DIR}/'.")
        return

    rows = []
    with open(archive_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    uids_to_restore = [r["uid"] for r in rows if (r.get("final_action") or "").strip().upper() == "DELETE"]
    print(f"   • Total rows in archive     : {len(rows)}")
    print(f"   • Emails marked for RESTORE : {len(uids_to_restore)}")

    if not uids_to_restore:
        print("✅ No deleted emails found in this archive.")
        return

    if dry_run:
        print(f"\n🔍 [DRY RUN PREVIEW] Would restore {len(uids_to_restore)} emails back to Inbox:")
        for r in [row for row in rows if (row.get("final_action") or "").strip().upper() == "DELETE"][:15]:
            print(f"   ↩️ UID {r['uid']} | {r.get('from', '')[:25]} | {r.get('subject', '')[:45]}")
        print("\nRun without '--dry-run' to execute restoration.")
        return

    print("\nConnecting to Gmail Trash folder...")
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(GMAIL_USER, GMAIL_APP_PASSWORD)

    # Select Gmail Trash folder (or Bin depending on locale)
    trash_folder = "[Gmail]/Trash"
    status, _ = mail.select(trash_folder)
    if status != "OK":
        trash_folder = "[Gmail]/Bin"
        mail.select(trash_folder)

    TRASH_BATCH_SIZE = 50
    restored_count = 0

    for i in range(0, len(uids_to_restore), TRASH_BATCH_SIZE):
        batch = uids_to_restore[i:i + TRASH_BATCH_SIZE]
        uid_set = ",".join(batch)
        # Remove Trash label and re-add to Inbox
        mail.uid("store", uid_set, "-X-GM-LABELS", "\\Trash")
        status, _ = mail.uid("store", uid_set, "+X-GM-LABELS", "\\Inbox")
        if status == "OK":
            restored_count += len(batch)
        else:
            print(f"⚠️ Warning: Could not untrash UID batch: {uid_set}")

    mail.close()
    mail.logout()

    print(f"🎉 Successfully restored {restored_count} emails back to your Inbox!")

    # Mark the archive file as restored
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    restored_name = os.path.join(PROCESSED_DIR, f"restored_{timestamp}_{os.path.basename(archive_file)}")
    try:
        shutil.move(archive_file, restored_name)
        print(f"📦 Renamed archive to: {restored_name}")
    except Exception as e:
        print(f"⚠️ Notice: Could not rename archive: {e}")
# ===============================================================


# ==================== CLI DISPATCHER ====================
def main():
    parser = argparse.ArgumentParser(
        description="Gmail AI Email Cleaner - Hardened Multi-Stage Pipeline with LLM Auditor & Undo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Examples:
  # 1. High-speed scan and auto-validate next 200 emails:
  python script.py scan --limit 200 --auto-validate

  # 2. Run LLM Auditor separately on the latest pending review:
  python script.py validate

  # 3. Simulate applying deletions:
  python script.py apply --dry-run

  # 4. Permanently apply reviewed deletions:
  python script.py apply

  # 5. Undo / restore previously deleted emails back to Inbox:
  python script.py restore
"""
    )
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")

    # Scan command
    scan_parser = subparsers.add_parser("scan", help="Scan inbox, classify emails with Gemini, and export to CSV")
    scan_parser.add_argument("--limit", type=int, default=100, help="Maximum number of emails to scan (default: 100)")
    scan_parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Concurrent worker threads (default: {DEFAULT_MAX_WORKERS}, max recommended: 12-14)"
    )
    scan_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of emails per Gemini prompt (default: {DEFAULT_BATCH_SIZE})"
    )
    scan_parser.add_argument(
        "--snippet-length",
        type=int,
        default=DEFAULT_SNIPPET_LENGTH,
        help=f"Max characters of plain text snippet (default: {DEFAULT_SNIPPET_LENGTH})"
    )
    scan_parser.add_argument(
        "--direction",
        choices=["oldest-first", "newest-first"],
        default="oldest-first",
        help="Scan order (default: oldest-first)"
    )
    scan_parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output CSV path (default: outputs/pending_review/pending_review_<timestamp>.csv)"
    )
    scan_parser.add_argument(
        "--reset-cursor",
        action="store_true",
        help=f"Reset cursor in {STATE_FILE} to re-scan from the beginning"
    )
    scan_parser.add_argument(
        "--auto-validate",
        action="store_true",
        help="Automatically trigger LLM False-Positive Auditor immediately after scan"
    )

    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Audit pending deletion candidates with LLM to catch false positives")
    validate_parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Pending review CSV to validate (default: most recent file in outputs/pending_review/)"
    )
    validate_parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help=f"Concurrent worker threads (default: {DEFAULT_MAX_WORKERS})"
    )
    validate_parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size for validation (default: {DEFAULT_BATCH_SIZE})"
    )

    # Apply command
    apply_parser = subparsers.add_parser("apply", help="Execute deletion for emails marked DELETE in the reviewed CSV")
    apply_parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Reviewed CSV file to process (default: most recent file in outputs/pending_review/)"
    )
    apply_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate deletion without making changes in Gmail"
    )

    # Restore command
    restore_parser = subparsers.add_parser("restore", help="Undo deletion and restore emails from Trash back to Inbox")
    restore_parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Completed review CSV to restore from (default: most recent in outputs/processed/)"
    )
    restore_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview emails that would be restored without modifying Gmail"
    )

    args = parser.parse_args()

    if args.command == "scan":
        run_scan(
            limit=args.limit,
            direction=args.direction,
            output_file=args.output,
            reset_cursor=args.reset_cursor,
            batch_size=args.batch_size,
            workers=args.workers,
            snippet_length=args.snippet_length,
            auto_validate=args.auto_validate
        )
    elif args.command == "validate":
        run_validate(
            review_file=args.file,
            workers=args.workers,
            batch_size=args.batch_size
        )
    elif args.command == "apply":
        run_apply(
            review_file=args.file,
            dry_run=args.dry_run
        )
    elif args.command == "restore":
        run_restore(
            archive_file=args.file,
            dry_run=args.dry_run
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()