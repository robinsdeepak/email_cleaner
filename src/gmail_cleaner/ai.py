"""Google Gemini AI integration, structured prompt schemas, and resilient parsing."""

import json
import time
from google import genai
from google.genai import types
from gmail_cleaner.config import MODEL_NAME, GEMINI_API_KEY, validate_gemini_credentials


def get_genai_client(api_key=None):
    """Returns an authenticated Gemini client."""
    key = validate_gemini_credentials(api_key or GEMINI_API_KEY)
    return genai.Client(api_key=key)


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


def classify_batch_with_gemini(client, batch_payload, max_retries=3):
    """Sends batch of emails to Gemini for KEEP/DELETE triage."""
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
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                print(f"   ⏳ Throttled (429). Retrying batch in {backoff_time}s...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    print(f"⚠️ API Error processing batch after {max_retries} retries: {e}")
                else:
                    print(f"⚠️ API Error processing batch with Gemini: {e}")
                return []
    return []


def audit_batch_with_gemini(client, batch_payload, max_retries=3):
    """Audits candidate deletions specifically to rescue false positives."""
    prompt = f"""
You are an expert, highly conservative Email Safety Auditor.
A first-pass system proposed to DELETE the following emails.
Your objective is to CATCH FALSE POSITIVES and rescue any critical emails that should NOT be deleted.

AUDIT RULES:
- OVERRIDE_KEEP: Select this if the email contains ANY of the following:
  1. Financial/Transactional: Receipts, invoices, payment confirmations, bank statements, tax documents, wire transfers.
  2. Account & Security: Password reset, 2FA/MFA verification codes, security alerts, login notices.
  3. Bookings & Travel: Flight itineraries, train/bus tickets, hotel reservations, event tickets, appointment confirmations.
  4. Logistics: Order tracking, shipping confirmations, delivery notices.
  5. Personal & Direct: Actual human-to-human personal or professional conversations, job interview requests, contracts.
  6. Ambiguous/Uncertain: If you have ANY doubt whether an email might be important, RESCUE IT.

- CONFIRMED_DELETE: Select this ONLY if you are 100% confident the email is purely:
  - Promotional marketing, discount coupons, sales pitches, periodic newsletter digests, product advertisements, cold spam.

FORMAT:
- validator_decision: either 'OVERRIDE_KEEP' or 'CONFIRMED_DELETE'
- validator_reason: extremely brief rationale (<10 words)

Candidate Emails:
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
                                    "enum": ["OVERRIDE_KEEP", "CONFIRMED_DELETE"]
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
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                print(f"   ⏳ Throttled (429). Retrying audit batch in {backoff_time}s...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    print(f"⚠️ Auditor API Error after {max_retries} retries: {e}")
                else:
                    print(f"⚠️ Auditor API Error with Gemini: {e}")
                return []
    return []
