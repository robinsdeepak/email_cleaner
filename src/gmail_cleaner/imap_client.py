"""Dedicated IMAP SSL client and email content parsing utilities."""

import email
from email.header import decode_header
from html.parser import HTMLParser
import imaplib
import quopri
import re
import sys
import time
from gmail_cleaner.config import (
    IMAP_SERVER,
    DEFAULT_SNIPPET_LENGTH,
    validate_imap_credentials,
)
from gmail_cleaner.logger import get_logger

logger = get_logger("imap")


def connect_imap(email_user=None, app_password=None):
    """Establishes a single dedicated SSL connection to Gmail IMAP."""
    user, pwd = validate_imap_credentials(email_user, app_password)
    logger.debug(f"Connecting to IMAP SSL server {IMAP_SERVER}:993 as '{user}'...")
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(user, pwd)
        logger.debug(f"Successfully authenticated with IMAP server as '{user}'.")
        return mail
    except imaplib.IMAP4.error as e:
        err_msg = str(e)
        if "AUTHENTICATIONFAILED" in err_msg:
            logger.error(f"IMAP Authentication Failed for '{user}'.")
            print(f"\n❌ IMAP Authentication Failed for '{user}'.")
            print("   Common causes:")
            print("   1. You must use a 16-character Google App Password (NOT your regular account password).")
            print("   2. 2-Step Verification must be enabled: https://myaccount.google.com/apppasswords")
            print("   3. IMAP must be enabled in Gmail Settings -> Forwarding and POP/IMAP.")
        else:
            logger.error(f"IMAP Connection Error for '{user}': {e}")
            print(f"❌ IMAP Connection Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected IMAP Error for '{user}': {e}")
        print(f"❌ Unexpected IMAP Error: {e}")
        raise


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


def clean_raw_text_snippet(raw_bytes, max_chars=DEFAULT_SNIPPET_LENGTH):
    """Cleans a raw partial text slice (handling quoted-printable, HTML tags, and whitespace)."""
    if not raw_bytes:
        return ""
    try:
        decoded_bytes = quopri.decodestring(raw_bytes)
        text = decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        text = raw_bytes.decode("utf-8", errors="ignore") if isinstance(raw_bytes, bytes) else str(raw_bytes)

    if "<" in text and ">" in text:
        parser = SimpleHTMLTextExtractor()
        try:
            parser.feed(text)
            text = parser.get_text()
        except Exception:
            pass

    clean_text = " ".join(text.split())
    return clean_text[:max_chars]


def fetch_batch_uids_fast(mail, uids_batch, snippet_length=DEFAULT_SNIPPET_LENGTH):
    """
    Fetches a batch of UIDs in 1 single IMAP command using partial body slicing.
    Downloads only headers + the first 1,000 bytes of body text (0 attachments).
    Returns a list of parsed email dicts.
    """
    if not uids_batch:
        return []

    t0 = time.time()
    logger.debug(f"Fetching IMAP batch of {len(uids_batch)} UIDs (range: {uids_batch[0]}..{uids_batch[-1]})...")
    uid_str = ",".join(str(u) for u in uids_batch)
    status, response = mail.uid("fetch", uid_str, "(FLAGS BODY.PEEK[HEADER] BODY.PEEK[TEXT]<0.1000>)")
    if status != "OK" or not response:
        logger.warning(f"IMAP fetch command failed: status={status} for UIDs {uids_batch[0]}..{uids_batch[-1]}")
        return []

    messages = {}
    current_uid = None

    for part in response:
        if isinstance(part, tuple):
            meta = part[0].decode("utf-8", errors="ignore")
            uid_match = re.search(r"UID\s+(\d+)", meta)
            if uid_match:
                current_uid = int(uid_match.group(1))
                if current_uid not in messages:
                    messages[current_uid] = {"raw_flags": b"", "header": b"", "text": b""}

            if current_uid:
                if "FLAGS" in meta:
                    flags_match = re.search(r"FLAGS\s+\(([^)]*)\)", meta)
                    if flags_match:
                        messages[current_uid]["raw_flags"] = flags_match.group(1).encode("utf-8")
                if "BODY[HEADER]" in meta:
                    messages[current_uid]["header"] = part[1]
                elif "BODY[TEXT]" in meta:
                    messages[current_uid]["text"] = part[1]

    results = []
    for uid in uids_batch:
        m = messages.get(uid)
        if not m or not m["header"]:
            continue

        try:
            msg_obj = email.message_from_bytes(m["header"])
            sender = get_decoded_header(msg_obj, "From")
            subject = get_decoded_header(msg_obj, "Subject")
            date_str = get_decoded_header(msg_obj, "Date")
            message_id = (msg_obj.get("Message-ID") or "").strip()
            snippet = clean_raw_text_snippet(m["text"], max_chars=snippet_length)
        except Exception:
            continue

        is_starred = b"\\Flagged" in m["raw_flags"] or b"Flagged" in m["raw_flags"]
        has_reply = bool(msg_obj.get("In-Reply-To") or msg_obj.get("References"))

        results.append({
            "uid": str(uid),
            "message_id": message_id,
            "date": date_str,
            "from": sender,
            "subject": subject,
            "snippet": snippet,
            "is_starred": "TRUE" if is_starred else "FALSE",
            "is_reply": "TRUE" if has_reply else "FALSE",
        })

    elapsed = time.time() - t0
    logger.debug(f"IMAP batch completed: parsed {len(results)}/{len(uids_batch)} UIDs in {elapsed:.3f}s")
    return results
