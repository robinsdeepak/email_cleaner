import argparse
import concurrent.futures
from datetime import datetime
import os
import sys
import time
from google import genai
import dotenv

dotenv.load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

if not GEMINI_API_KEY or GEMINI_API_KEY == "your-gemini-api-key":
    print("❌ Error: GEMINI_API_KEY is not set in .env")
    sys.exit(1)


# ==================== TEST 1: RPM STRESS TEST ====================
def stress_test_rpm(client, target_requests=2200, concurrency=80):
    """
    Fires target_requests (default: 2,200) minimal 1-token requests as fast as possible
    to test the 2,000 Requests-Per-Minute (RPM) ceiling.
    Cost: < $0.002 (fraction of a cent).
    """
    print("\n" + "=" * 60)
    print("🚀 [TEST 1: STRESS TESTING RPM (TARGET > 2,000 RPM)]")
    print(f"Target Requests : {target_requests}")
    print(f"Concurrency     : {concurrency} worker threads")
    print(f"Payload         : 1-token minimal ping (cost < $0.002)")
    print("=" * 60)

    success_count = 0
    rate_limit_429 = 0
    other_errors = 0
    first_error_msg = None

    start_time = time.time()

    def send_quick_request(req_id):
        nonlocal first_error_msg
        try:
            client.models.generate_content(
                model=MODEL_NAME,
                contents="1",
                config={"max_output_tokens": 1}
            )
            return True, None
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "resource_exhausted" in err_str.lower():
                if not first_error_msg:
                    first_error_msg = err_str
                return False, "429"
            return False, err_str

    print("Firing requests...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(send_quick_request, i) for i in range(target_requests)]
        
        completed = 0
        for f in concurrent.futures.as_completed(futures):
            ok, err_type = f.result()
            completed += 1
            if ok:
                success_count += 1
            elif err_type == "429":
                rate_limit_429 += 1
            else:
                other_errors += 1
            
            # Print live progress every 200 requests
            if completed % 200 == 0 or completed == target_requests:
                elapsed = time.time() - start_time
                current_rpm = (completed / elapsed) * 60 if elapsed > 0 else 0
                print(f"   [{completed}/{target_requests}] | OK: {success_count} | 429: {rate_limit_429} | Current Pace: {current_rpm:.0f} RPM")

    elapsed_total = time.time() - start_time
    effective_rpm = (target_requests / elapsed_total) * 60 if elapsed_total > 0 else 0

    print("\n--- RPM Test Results ---")
    print(f"Total Requests Sent   : {target_requests}")
    print(f"Duration              : {elapsed_total:.2f} seconds")
    print(f"Peak Sustained Pace   : {effective_rpm:.0f} RPM")
    print(f"Successful (200 OK)   : {success_count}")
    print(f"Throttled (429)       : {rate_limit_429}")
    print(f"Other Errors          : {other_errors}")

    if rate_limit_429 > 0:
        print(f"\n🔴 HIT RPM CEILING at pace ~{effective_rpm:.0f} RPM.")
        if first_error_msg:
            print(f"Sample 429 Response:\n{first_error_msg[:300]}...")
    else:
        print(f"\n🟢 PASSED! Sustained {effective_rpm:.0f} RPM with ZERO 429 throttling!")


# ==================== TEST 2: TPM STRESS TEST ====================
def stress_test_tpm(client, target_tokens=4500000, num_parallel_requests=6):
    """
    Fires parallel requests with large payloads to test the 4,000,000 Tokens-Per-Minute (TPM) limit.
    Cost: ~ $0.40 - $0.50 (for ~4.5M input tokens).
    """
    print("\n" + "=" * 60)
    print("🌊 [TEST 2: STRESS TESTING TPM (TARGET > 4,000,000 TPM)]")
    print(f"Target Total Tokens: ~{target_tokens:,} tokens")
    print(f"Number of Requests : {num_parallel_requests} parallel large prompts")
    print(f"Approximate Cost   : ~$0.45 (at $0.10 / 1M input tokens)")
    print("=" * 60)

    tokens_per_request = target_tokens // num_parallel_requests
    # Approximately 4 chars per token for repeated english text
    chars_needed = tokens_per_request * 4
    base_phrase = "Alpha Beta Gamma Delta Epsilon Zeta Eta Theta Iota Kappa Lambda Mu Nu Xi Omicron Pi Rho Sigma Tau Upsilon Phi Chi Psi Omega. "
    repeat_count = (chars_needed // len(base_phrase)) + 1
    large_text = base_phrase * repeat_count

    print(f"Generating payload of ~{len(large_text):,} characters per request...")
    # Verify exact tokens with count_tokens
    sample_count = client.models.count_tokens(model=MODEL_NAME, contents=large_text)
    actual_tokens_per_req = sample_count.total_tokens
    actual_total_tokens = actual_tokens_per_req * num_parallel_requests
    print(f"Measured tokens per request: {actual_tokens_per_req:,} tokens")
    print(f"Total burst volume         : {actual_total_tokens:,} tokens across {num_parallel_requests} requests\n")

    start_time = time.time()
    success_count = 0
    tpm_429_count = 0
    other_errors = 0
    first_error_msg = None

    def send_large_request(req_id):
        nonlocal first_error_msg
        try:
            print(f"   [Worker {req_id + 1}] Sending {actual_tokens_per_req:,} tokens...", flush=True)
            client.models.generate_content(
                model=MODEL_NAME,
                contents=large_text,
                config={"max_output_tokens": 1}
            )
            print(f"   [Worker {req_id + 1}] ✅ Succeeded!", flush=True)
            return True, None
        except Exception as e:
            err_str = str(e)
            print(f"   [Worker {req_id + 1}] ❌ Error: {err_str[:120]}...", flush=True)
            if "429" in err_str or "resource_exhausted" in err_str.lower():
                if not first_error_msg:
                    first_error_msg = err_str
                return False, "429"
            return False, err_str

    print("Dispatching all large requests concurrently...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallel_requests) as executor:
        futures = [executor.submit(send_large_request, i) for i in range(num_parallel_requests)]
        for f in concurrent.futures.as_completed(futures):
            ok, err_type = f.result()
            if ok:
                success_count += 1
            elif err_type == "429":
                tpm_429_count += 1
            else:
                other_errors += 1

    elapsed = time.time() - start_time
    tokens_consumed = success_count * actual_tokens_per_req
    tokens_attempted = num_parallel_requests * actual_tokens_per_req
    effective_tpm = (tokens_consumed / elapsed) * 60 if elapsed > 0 else 0

    print("\n--- TPM Test Results ---")
    print(f"Duration               : {elapsed:.2f} seconds")
    print(f"Total Tokens Attempted : {tokens_attempted:,} tokens")
    print(f"Successful Tokens      : {tokens_consumed:,} tokens")
    print(f"Effective Processed TPM: {effective_tpm:,.0f} TPM")
    print(f"Successful Requests    : {success_count}/{num_parallel_requests}")
    print(f"Throttled Requests     : {tpm_429_count}")

    if tpm_429_count > 0:
        print(f"\n🔴 HIT TPM CEILING (> 4M TPM).")
        if first_error_msg:
            print(f"Sample 429 Response:\n{first_error_msg[:350]}...")
    else:
        print(f"\n🟢 PASSED! Successfully pushed {tokens_consumed:,} tokens in {elapsed:.1f}s without 429!")


def main():
    parser = argparse.ArgumentParser(description="Stress test Gemini 2,000+ RPM and 4M+ TPM limits")
    parser.add_argument("--test", choices=["rpm", "tpm", "all"], default="all", help="Which stress test to execute")
    parser.add_argument("--rpm-count", type=int, default=2200, help="Number of requests for RPM test (default: 2200)")
    parser.add_argument("--tpm-tokens", type=int, default=4500000, help="Target total tokens for TPM test (default: 4.5M)")
    args = parser.parse_args()

    client = genai.Client(api_key=GEMINI_API_KEY)

    if args.test in ["rpm", "all"]:
        stress_test_rpm(client, target_requests=args.rpm_count)
    
    if args.test in ["tpm", "all"]:
        # Small cooldown between tests
        if args.test == "all":
            print("\nPausing 5 seconds before TPM test...")
            time.sleep(5)
        stress_test_tpm(client, target_tokens=args.tpm_tokens)


if __name__ == "__main__":
    main()
