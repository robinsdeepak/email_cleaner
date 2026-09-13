"""Dedicated IMAP SSL client and email content parsing utilities."""

from email.header import decode_header
from html.parser import HTMLParser
import imaplib
import sys
from gmail_cleaner.config import (
    IMAP_SERVER,
    DEFAULT_SNIPPET_LENGTH,
    validate_imap_credentials,
)


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
