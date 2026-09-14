"""Google Gemini AI integration, structured prompt schemas, and resilient parsing."""

import json
import time
from google import genai
from google.genai import types
from gmail_cleaner.config import MODEL_NAME, GEMINI_API_KEY, validate_gemini_credentials
from gmail_cleaner.logger import get_logger

logger = get_logger("ai")


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
    if not batch_payload:
        return []

    first_id = batch_payload[0].get("id", "")
    last_id = batch_payload[-1].get("id", "")
    logger.debug(f"Gemini classify request dispatched: {len(batch_payload)} emails (UIDs {first_id}..{last_id}) using {MODEL_NAME}")

    prompt = f"""
Analyze this batch of emails and classify each as DELETE (True) or KEEP (False).

STRICT CLASSIFICATION CRITERIA:

🗑️ DELETE (True) - SAFE TO DISCARD:
1. All OTPs & Verification Codes: Single-use OTPs, 2FA/MFA security codes, password reset links, login alerts, sign-in notices (these are ephemeral and can always be resent if needed).
2. Account Onboarding & Verification: "Welcome to [App]", "Getting started", "Verify your email", "Confirm subscription" (unless containing an explicit software license key or API secret).
3. Automated Job Alerts & Digests: Job recommendations, matching alerts ("10 new jobs for you", "Jobs matching your profile"), recruiter mass-mailers, platform alerts ("X viewed your profile") from Indeed, Glassdoor, Instahyre, Naukri, TechGig, Google Jobs, LinkedIn. (KEEP only direct 1-to-1 human recruiter interview scheduling).
4. Food Delivery & Transit Churn: Order updates, delivery status ("Your order was delivered superfast!", "Food is in safe hands"), ride confirmations from Swiggy, Zomato, Uber, Ola.
5. Shipping & Tracking Status: "Package out for delivery", "Item has been shipped", "Delivery feedback request" from Amazon, Myntra, Flipkart.
6. Promotional & Marketing: Marketing discounts, sales pitches, periodic newsletters, property listings (Magicbricks, 99acres), webinar invites, cold spam.
7. Educational & Contest Reminders: Course progress reminders ("Finish week 2", "5 days left to enroll"), automated coding contest announcements (Codeforces, HackerEarth).

🛡️ KEEP (False) - PERMANENT VALUE ONLY:
1. Official Financial Records: Tax documents (ITR, Form 16, TDS), monthly bank statements, mutual fund/demat statements, salary/payroll slips, insurance policies.
2. Formal Purchase Invoices: Primary receipts with explicit monetary amount paid, transaction ID, and itemized billing for durable products, electronics, subscriptions, or software licenses.
3. Travel & Bookings: Official flight tickets, hotel reservations, train bookings, event passes.
4. Legal & Compliance: Government notices, company registration, contracts, legal compliance.
5. Personal & Human Correspondence: Genuine 1-to-1 human-written personal or professional conversations, direct job offer letters.

FORMAT:
- Keep 'reason' extremely brief (under 10 words).

Emails:
{json.dumps(batch_payload, indent=2)}
"""
    for attempt in range(max_retries + 1):
        try:
            t0 = time.time()
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

    prompt = f"""
You are an expert Email Safety Auditor.
A first-pass system proposed to DELETE the following candidate emails.
Your objective is to CATCH REAL FALSE POSITIVES (rescue critical permanent documents) while CONFIRMING deletion for ephemeral clutter.

AUDIT RULES:

❌ CONFIRMED_DELETE (Approve deletion; do NOT rescue):
1. All OTPs & Security Codes: Single-use OTPs, 2FA/MFA verification codes, login alerts, password resets (these are ephemeral and can always be resent if needed).
2. Account Onboarding & Verification: "Welcome to [Service]", "Verify your email", onboarding drips.
3. Automated Job Alerts & Recruitment Spam: Automated matching digests ("10 new jobs for you", "Jobs matching your profile"), recruiter mass-mailers, platform alerts ("X viewed your profile") from Indeed, Glassdoor, Instahyre, Naukri, TechGig, LinkedIn. (These are NOT personal 1-to-1 interview scheduling).
4. Food Delivery & Transit Churn: Order updates, delivery tracking ("Your order was delivered superfast!", "Food is in safe hands") from Swiggy, Zomato, Uber, Ola.
5. Shipping & Status Updates: E-commerce package tracking ("Item shipped", "Delivered") where the master purchase invoice is separate.
6. Marketing & Announcements: Discounts, sales pitches, webinars, real estate listings (Magicbricks), weekly digests.
7. Educational Reminders: Course progress reminders, automated coding contest announcements.

🛡️ OVERRIDE_KEEP (Rescue immediately):
1. Official Financial: Monthly bank statements, tax filings (ITR/Form 16/TDS), mutual fund/demat statements, salary/payroll slips, insurance policies.
2. Formal Purchase Invoices: Official purchase receipts with explicit monetary amount paid, transaction ID, and itemized billing for durable products, electronics, subscriptions, or software licenses.
3. Travel & Bookings: Official flight tickets, train bookings, hotel reservations, event passes.
4. Legal & Compliance: Government notices, company registration, contracts, legal compliance.
5. Personal Correspondence: Genuine 1-to-1 human-written personal or business conversations, direct job offer letters from a named recruiter.

FORMAT:
- validator_decision: either 'OVERRIDE_KEEP' or 'CONFIRMED_DELETE'
- validator_reason: extremely brief rationale (<10 words)

Candidate Emails:
{json.dumps(batch_payload, indent=2)}
"""
    for attempt in range(max_retries + 1):
        try:
            t0 = time.time()
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
