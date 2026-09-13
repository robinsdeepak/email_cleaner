"""Configuration management and credential validation."""

import os
import sys
import warnings
import dotenv

# Suppress Google GenAI SDK automatic function calling advisory warning
warnings.filterwarnings("ignore", message=".*Direct use of automatic function calling.*")

# Load environment variables
dotenv.load_dotenv()

GMAIL_USER = os.getenv("GMAIL_USER", "your-email@gmail.com")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "your-16-char-app-password")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "your-gemini-api-key")

IMAP_SERVER = os.getenv("IMAP_SERVER", "imap.gmail.com")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

DEFAULT_BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
DEFAULT_MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
DEFAULT_SNIPPET_LENGTH = int(os.getenv("SNIPPET_LENGTH", "250"))
OUTPUTS_ROOT = os.getenv("OUTPUTS_ROOT", "outputs")


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
