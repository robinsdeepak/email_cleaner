import concurrent.futures
import os
import sys
import time
from google import genai
from google.genai.errors import APIError
import dotenv

dotenv.load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

if not GEMINI_API_KEY or GEMINI_API_KEY == "your-gemini-api-key":
    print("❌ Error: GEMINI_API_KEY is not set in .env")
    sys.exit(1)


def send_probe_request(client, req_id):
    """Sends a minimal 1-token probe request to test rate limits with near-zero cost."""
    try:
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents="ping",
            config={"max_output_tokens": 1}
        )
        return True, None
    except Exception as e:
        error_str = str(e).lower()
        if "429" in error_str or "resource_exhausted" in error_str or "quota" in error_str:
            return False, "429_RATE_LIMIT"
        return False, f"ERROR: {e}"


def test_batch_burst(client, batch_size):
    """Fires batch_size requests concurrently and checks how many succeed without hitting 429."""
    print(f"   Testing burst of {batch_size} concurrent requests...", end="", flush=True)
    successes = 0
    rate_limited = 0
    other_errors = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = [executor.submit(send_probe_request, client, i) for i in range(batch_size)]
        for f in concurrent.futures.as_completed(futures):
            ok, err = f.result()
            if ok:
                successes += 1
            elif err == "429_RATE_LIMIT":
                rate_limited += 1
            else:
                other_errors += 1

    if rate_limited > 0:
        print(f" ❌ Hit 429 Rate Limit ({successes} OK, {rate_limited} limited)")
        return False, successes
    elif other_errors > 0:
        print(f" ⚠️ Encountered errors ({successes} OK, {other_errors} other errors)")
        return False, successes
    else:
        print(f" ✅ All {batch_size} succeeded!")
        return True, successes


def binary_search_rate_limit(client, low=8, high=32, cooldown_sec=4):
    """Uses binary search across burst sizes to pinpoint the exact rate limit threshold."""
    print(f"\n🔍 [BINARY SEARCH RATE LIMIT DETECTOR]")
    print(f"Model: {MODEL_NAME}")
    print(f"Search range: {low} to {high} concurrent requests\n")

    best_confirmed = 0
    
    # Fast initial probe: check if 20 requests succeed.
    # Google AI Studio Free Tier has a hard cap of 15 RPM.
    # If 20 passes, user is immediately confirmed on Paid / Tier 1+.
    print("👉 Step 1: Probing threshold boundary (20 requests)...")
    passed, ok_count = test_batch_burst(client, 20)
    
    if passed:
        print("\n🎉 Burst of 20 succeeded without 429!")
        print("   This confirms your API key is on **PAY-AS-YOU-GO / PAID TIER** (Quota: 1,000+ RPM).")
        print("   Searching higher bounds to check high-concurrency capability...")
        low = 20
        high = 60
        best_confirmed = 20
    else:
        print("\n⚠️ Request failed or hit 429 around 20 requests.")
        print("   This indicates a **FREE TIER** quota (Standard: 15 RPM).")
        print("   Narrowing down exact limit with binary search...")
        high = 19
        best_confirmed = ok_count

    # Binary search within [low, high]
    print(f"\n👉 Step 2: Binary Search in range [{low}, {high}]:")
    
    while low <= high:
        mid = (low + high) // 2
        
        # Brief cooldown between iterations to avoid rolling-window 429 contamination
        time.sleep(cooldown_sec)
        
        passed, ok_count = test_batch_burst(client, mid)
        if passed:
            best_confirmed = max(best_confirmed, mid)
            low = mid + 1  # Try higher
        else:
            high = mid - 1  # Rate limited, search lower

    return best_confirmed


def main():
    client = genai.Client(api_key=GEMINI_API_KEY)
    
    start_time = time.time()
    max_burst = binary_search_rate_limit(client, low=10, high=35)
    total_time = time.time() - start_time

    print("\n" + "=" * 55)
    print("📊 RATE LIMIT TEST RESULTS")
    print("=" * 55)
    print(f"Max Confirmed Concurrent Burst : {max_burst} requests")
    print(f"Test Duration                 : {total_time:.1f} seconds")
    
    if max_burst >= 20:
        print("\n🟢 DETECTED TIER: **PAY-AS-YOU-GO (PAID TIER)**")
        print("   • Standard Rate Limits: 1,000 to 2,000 RPM | 4,000,000 TPM")
        print("   • Recommended script.py settings:")
        print("     - MAX_WORKERS = 8 to 12")
        print("     - BATCH_SIZE  = 25")
        print("   • You can process thousands of emails quickly without throttle.")
    else:
        print("\n🟡 DETECTED TIER: **FREE TIER**")
        print("   • Standard Rate Limits: 15 RPM | 1,000,000 TPM | 1,500 RPD")
        print("   • Recommended script.py settings:")
        print("     - MAX_WORKERS = 4 (default, safely under 15 RPM)")
        print("     - BATCH_SIZE  = 25")
        print("   • Capacity: up to ~375 emails/min and 37,500 emails/day.")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()
