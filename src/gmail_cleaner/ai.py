"""Google Gemini AI integration, structured prompt schemas, and resilient parsing."""

import json
import time
from google import genai
from google.genai import types
from gmail_cleaner.config import (
    MODEL_NAME,
    GEMINI_API_KEY,
    validate_gemini_credentials,
    THINKING_BUDGET,
    TEMPERATURE,
)
from gmail_cleaner.logger import get_logger

logger = get_logger("ai")


def get_genai_client(api_key=None):
    """Returns an authenticated Gemini client."""
    key = validate_gemini_credentials(api_key or GEMINI_API_KEY)
    return genai.Client(api_key=key)


def safe_parse_json_array(raw_text):
    """Safely parses JSON array or object, automatically repairing truncated output if string was cut off."""
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
        data = json.loads(text)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
    except json.JSONDecodeError:
        last_brace = text.rfind("}")
        if last_brace != -1:
            repaired = text[:last_brace + 1]
            if text.startswith("["):
                repaired += "\n]"
            try:
                data = json.loads(repaired)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return [data]
            except Exception:
                pass
    return []


def classify_batch_with_gemini(client, batch_payload, max_retries=3):
    """Sends batch of emails to Gemini for KEEP/DELETE triage."""
    if not batch_payload:
        return []

    first_id = batch_payload[0].get("id", "")
    last_id = batch_payload[-1].get("id", "")
    logger.debug(f"Gemini classify request dispatched: {len(batch_payload)} emails (UIDs {first_id}..{last_id}) using {MODEL_NAME}")

    header_text = "Analyze this email and classify with status, confidence, and category." if len(batch_payload) == 1 else "Analyze this batch of emails and classify each with status, confidence, and category."
    prompt = f"""
{header_text}

MULTI-STATUS CLASSIFICATION TAXONOMY:

1. 🗑️ CONFIDENT_DELETE:
   - High confidence disposable clutter.
   - All OTPs & 2FA security codes, login alerts, password resets.
   - Automated recruiter mailers, mass job digests (Indeed, Glassdoor, Naukri, LinkedIn, TechGig).
   - Food delivery & ride status updates (Swiggy, Zomato, Uber, Ola).
   - Commercial marketing, sales pitches, real estate blasts, discount newsletters.

2. ⚠️ PROBABLE_DELETE:
   - Likely safe to delete, but has minor user notification value.
   - Service onboarding welcome emails without license keys or passwords ("Welcome to Canva").
   - E-commerce shipping/tracking notifications where the separate tax invoice was already received.
   - Educational contest reminders, weekly progress emails.

3. 🟡 NEEDS_REVIEW:
   - Borderline, ambiguous, or incomplete information where automated deletion is risky.
   - Account changes, legal terms updates, contract renewals, customer service thread updates.
   - Ambiguous transaction notices lacking clear receipt metadata.

4. 🛡️ PROBABLE_KEEP:
   - Likely valuable correspondence or records.
   - Account registration containing login credentials, software keys, or API tokens.
   - Travel inquiries, upcoming event invitations, human personal newsletters.

5. 💎 CONFIDENT_KEEP:
   - Critical permanent financial, legal, or personal records.
   - Monthly bank statements, tax forms (ITR, Form 16, TDS), mutual fund/demat statements, salary slips.
   - Official purchase invoices with transaction ID, itemized billing, and monetary amount paid.
   - Official flight tickets, train bookings, hotel vouchers.
   - Direct 1-to-1 human recruiter emails or employment offer letters.

FORMAT & ENUMS:
- status: exactly one of ["CONFIDENT_DELETE", "PROBABLE_DELETE", "NEEDS_REVIEW", "PROBABLE_KEEP", "CONFIDENT_KEEP"]
- confidence: exactly one of ["HIGH", "MEDIUM", "LOW"]
- category: exactly one of ["FINANCIAL", "INVOICE", "TRAVEL", "SECURITY_OTP", "JOB_ALERT", "FOOD_TRANSIT", "MARKETING", "PERSONAL", "OTHER"]
- reason: extremely brief rationale (< 10 words)
- delete: boolean (true for CONFIDENT_DELETE and PROBABLE_DELETE, false otherwise)

{"Email:" if len(batch_payload) == 1 else "Emails:"}
{json.dumps(batch_payload, indent=2)}
"""
    output_cap = min(8192, max(400, len(batch_payload) * 90))
    for attempt in range(max_retries + 1):
        try:
            t0 = time.time()
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=TEMPERATURE,
                    thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET),
                    max_output_tokens=output_cap,
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
                    response_schema={
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "id": {"type": "STRING"},
                                "status": {
                                    "type": "STRING",
                                    "enum": ["CONFIDENT_DELETE", "PROBABLE_DELETE", "NEEDS_REVIEW", "PROBABLE_KEEP", "CONFIDENT_KEEP"]
                                },
                                "confidence": {
                                    "type": "STRING",
                                    "enum": ["HIGH", "MEDIUM", "LOW"]
                                },
                                "category": {
                                    "type": "STRING",
                                    "enum": ["FINANCIAL", "INVOICE", "TRAVEL", "SECURITY_OTP", "JOB_ALERT", "FOOD_TRANSIT", "MARKETING", "PERSONAL", "OTHER"]
                                },
                                "delete": {"type": "BOOLEAN"},
                                "reason": {"type": "STRING"}
                            },
                            "required": ["id", "status", "confidence", "category", "delete", "reason"]
                        }
                    }
                )
            )
            elapsed = time.time() - t0
            parsed = safe_parse_json_array(response.text)
            if parsed:
                del_count = sum(1 for p in parsed if p.get("delete"))
                logger.debug(f"Gemini classify response parsed in {elapsed:.2f}s: {len(parsed)} results ({del_count} DELETE, {len(parsed) - del_count} KEEP)")
                return parsed
            else:
                raw_preview = (response.text or "")[:150].replace("\n", " ")
                logger.warning(f"Failed to parse JSON response from Gemini (attempt {attempt + 1}/{max_retries}). Raw preview: {raw_preview}")
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                logger.warning(f"   ⏳ Throttled (429/ResourceExhausted). Retrying batch in {backoff_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    logger.error(f"⚠️ API Error processing batch after {max_retries} retries: {e}", exc_info=True)
                else:
                    logger.error(f"⚠️ API Error processing batch with Gemini (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                return []
    return []


def audit_batch_with_gemini(client, batch_payload, max_retries=3):
    """Audits candidate deletions specifically to rescue false positives."""
    if not batch_payload:
        return []

    first_id = batch_payload[0].get("id", "")
    last_id = batch_payload[-1].get("id", "")
    logger.debug(f"Gemini audit request dispatched: {len(batch_payload)} candidate deletes (UIDs {first_id}..{last_id}) using {MODEL_NAME}")

    candidate_header = "Candidate Email:" if len(batch_payload) == 1 else "Candidate Emails:"
    prompt = f"""
You are an expert Email Safety Auditor.
A first-pass system proposed to DELETE or REVIEW the following candidate email(s).
Your objective is to CATCH REAL FALSE POSITIVES (rescue critical permanent documents) and FLAG AMBIGUITIES for user review while CONFIRMING deletion for ephemeral clutter.

MULTI-STATUS VALIDATION VERDICTS:

1. ❌ CONFIRMED_DELETE (High Confidence Trash):
   - Single-use OTPs, 2FA security codes, login alerts, password resets.
   - Recruiter mass-mailers, platform job digests (Naukri, Indeed, LinkedIn, TechGig).
   - Swiggy/Zomato/Uber order updates, delivery tracking pings.
   - Sales pitches, discounts, real estate promotions, newsletter blasts.

2. ⚠️ SOFT_DELETE (Moderate Confidence Trash):
   - Secondary marketing, platform digests, educational webinar invites.
   - Safe to delete, but available if user wants to skim.

3. 🟡 NEEDS_USER_REVIEW (Ambiguous / User Decision Required):
   - Ambiguous transaction or reservation notices where itemization is missing or unclear.
   - Contract or account policy changes, service support ticket threads.
   - Borderline personal correspondence.

4. 🛡️ SUGGEST_KEEP (Moderate Rescue):
   - Non-critical purchase receipt, event registration with tickets/passes, upcoming appointment.

5. 💎 CONFIRMED_KEEP (Critical High-Certainty Rescue):
   - Official financial: bank statements, salary slips, tax filings (ITR, Form 16, TDS), mutual fund statements.
   - Formal purchase invoices: durable electronics, software licenses, explicit order receipts with monetary amounts.
   - Travel bookings: official airline tickets, train reservations, hotel vouchers.
   - Legal/government notices, direct 1-to-1 personal or job offer letters.

FORMAT & ENUMS:
- validator_status: exactly one of ["CONFIRMED_DELETE", "SOFT_DELETE", "NEEDS_USER_REVIEW", "SUGGEST_KEEP", "CONFIRMED_KEEP"]
- validator_confidence: exactly one of ["HIGH", "MEDIUM", "LOW"]
- validator_decision: either "OVERRIDE_KEEP" (for CONFIRMED_KEEP and SUGGEST_KEEP) or "CONFIRMED_DELETE"
- validator_reason: extremely brief rationale (< 10 words)

{candidate_header}
{json.dumps(batch_payload, indent=2)}
"""
    output_cap = min(8192, max(400, len(batch_payload) * 90))
    for attempt in range(max_retries + 1):
        try:
            t0 = time.time()
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=TEMPERATURE,
                    thinking_config=types.ThinkingConfig(thinking_budget=THINKING_BUDGET),
                    max_output_tokens=output_cap,
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
                    response_schema={
                        "type": "ARRAY",
                        "items": {
                            "type": "OBJECT",
                            "properties": {
                                "id": {"type": "STRING"},
                                "validator_status": {
                                    "type": "STRING",
                                    "enum": ["CONFIRMED_DELETE", "SOFT_DELETE", "NEEDS_USER_REVIEW", "SUGGEST_KEEP", "CONFIRMED_KEEP"]
                                },
                                "validator_confidence": {
                                    "type": "STRING",
                                    "enum": ["HIGH", "MEDIUM", "LOW"]
                                },
                                "validator_decision": {
                                    "type": "STRING",
                                    "enum": ["OVERRIDE_KEEP", "CONFIRMED_DELETE"]
                                },
                                "validator_reason": {"type": "STRING"}
                            },
                            "required": ["id", "validator_status", "validator_confidence", "validator_decision", "validator_reason"]
                        }
                    }
                )
            )
            elapsed = time.time() - t0
            parsed = safe_parse_json_array(response.text)
            if parsed:
                rescued_count = sum(1 for p in parsed if p.get("validator_decision") == "OVERRIDE_KEEP")
                logger.debug(f"Gemini audit response parsed in {elapsed:.2f}s: {len(parsed)} results ({rescued_count} RESCUED/KEEP, {len(parsed) - rescued_count} CONFIRMED_DELETE)")
                return parsed
            else:
                raw_preview = (response.text or "")[:150].replace("\n", " ")
                logger.warning(f"Failed to parse JSON response from Gemini Auditor (attempt {attempt + 1}/{max_retries}). Raw preview: {raw_preview}")
        except Exception as e:
            err_str = str(e).lower()
            if ("429" in err_str or "resource_exhausted" in err_str) and attempt < max_retries:
                backoff_time = (2 ** attempt) * 2
                logger.warning(f"   ⏳ Throttled (429/ResourceExhausted) in Auditor. Retrying batch in {backoff_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(backoff_time)
            else:
                if attempt == max_retries:
                    logger.error(f"⚠️ Auditor API Error after {max_retries} retries: {e}", exc_info=True)
                else:
                    logger.error(f"⚠️ Auditor API Error with Gemini (attempt {attempt + 1}/{max_retries}): {e}", exc_info=True)
                return []
    return []


def classify_single_email(client, email_dict, max_retries=3):
    """Classifies a single email individually to eliminate batch context contamination."""
    results = classify_batch_with_gemini(client, [email_dict], max_retries=max_retries)
    if results:
        return results[0]
    return {"id": email_dict.get("id", ""), "delete": False, "reason": "Classification failed"}


def audit_single_email(client, email_dict, max_retries=3):
    """Audits a single candidate email individually with zero batch bias."""
    results = audit_batch_with_gemini(client, [email_dict], max_retries=max_retries)
    if results:
        return results[0]
    return {"id": email_dict.get("id", ""), "validator_decision": "CONFIRMED_DELETE", "validator_reason": "Audit failed"}
