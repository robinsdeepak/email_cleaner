"""
CleanMail - Production-Grade AI Gmail Cleaner.
Inspired by Clean Email, Superhuman, and Linear.

Core Views:
1. 🧹 Inbox Clean (Home Cockpit - Cleanliness Score & 1-Click Clean)
2. 📦 Smart Bundles (Categorical Cleaning: Newsletters, Shopping, Social, OTPs)
3. 👥 Top Senders (High-Leverage Sender Clusters)
4. 🛡️ Trash & Undo (30-Day Retention Notice & 1-Click Restore)
5. ⚙️ Settings & Engine (Accounts, AI Tuning, DB Tools, Logs)
"""

import json
import os
import sys
import time
from datetime import datetime

# Ensure src/ is on sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import pandas as pd
import streamlit as st

from gmail_cleaner.config import GMAIL_USER, DEFAULT_BATCH_SIZE, DEFAULT_MAX_WORKERS
from gmail_cleaner.db import EmailDB, get_default_db_path
from gmail_cleaner.imap_client import test_imap_credentials
from gmail_cleaner.logger import get_logger
from gmail_cleaner.state import get_latest_artifact
from gmail_cleaner.worker import worker

# Pipeline operations
from gmail_cleaner.stages import run_delete, run_restore
from gmail_cleaner.streaming import run_streaming_pipeline

# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & LINEAR/SUPERHUMAN DESIGN SYSTEM
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="CleanMail",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Clean Container Elevation */
    .hero-container {
        border-radius: 16px;
        padding: 32px 28px;
        margin-bottom: 24px;
        border: 1px solid rgba(128, 128, 128, 0.16);
        background: linear-gradient(145deg, rgba(128, 128, 128, 0.04) 0%, rgba(128, 128, 128, 0.01) 100%);
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.03);
    }
    
    .clean-card {
        border-radius: 12px;
        padding: 20px 22px;
        margin-bottom: 14px;
        border: 1px solid rgba(128, 128, 128, 0.14);
        background: rgba(128, 128, 128, 0.03);
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.02);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .clean-card:hover {
        border-color: rgba(99, 102, 241, 0.4);
        box-shadow: 0 6px 16px rgba(0, 0, 0, 0.05);
    }

    /* Sender Avatar */
    .sender-avatar {
        width: 38px;
        height: 38px;
        border-radius: 50%;
        background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
        color: #ffffff;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 0.95rem;
        flex-shrink: 0;
    }

    /* Minimal Badges */
    .chip {
        display: inline-flex;
        align-items: center;
        padding: 3px 9px;
        border-radius: 20px;
        font-size: 0.78rem;
        font-weight: 600;
        letter-spacing: 0.2px;
    }
    .chip-del {
        background: rgba(239, 68, 68, 0.12);
        color: #ef4444;
    }
    .chip-keep {
        background: rgba(16, 185, 129, 0.12);
        color: #10b981;
    }
    .chip-review {
        background: rgba(245, 158, 11, 0.12);
        color: #f59e0b;
    }
    .chip-neutral {
        background: rgba(128, 128, 128, 0.12);
        color: inherit;
        opacity: 0.85;
    }

    /* Clean Primary & Secondary Buttons */
    .stButton>button {
        border-radius: 8px;
        font-weight: 600;
        font-size: 0.92rem;
        padding: 6px 16px;
        transition: all 0.15s ease;
    }
    
    /* Remove extraneous padding */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 3rem !important;
        max-width: 1120px !important;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# SIDEBAR: MINIMALIST BRAND & NAVIGATION
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### ✨ CleanMail")
    st.caption("Inbox Zero for Gmail")

    temp_db = EmailDB()
    configured_accounts = temp_db.list_accounts()

    if not configured_accounts:
        target_account = st.text_input("Active Account", value=GMAIL_USER)
    else:
        acc_emails = [acc["email"] for acc in configured_accounts]
        default_acc = next((a["email"] for a in configured_accounts if a.get("is_default")), acc_emails[0])
        current_selection = st.session_state.get("target_account", default_acc)
        if current_selection not in acc_emails:
            current_selection = default_acc

        selected_idx = acc_emails.index(current_selection) if current_selection in acc_emails else 0
        target_account = st.selectbox(
            "Account",
            options=acc_emails,
            index=selected_idx,
            key="sb_active_account_select",
            label_visibility="collapsed",
        )
        st.session_state["target_account"] = target_account

    db = EmailDB(account=target_account)
    db_file = db.db_path
    db_exists = os.path.isfile(db_file)

    st.markdown("<div style='margin: 12px 0;'></div>", unsafe_allow_html=True)

    # 5-View Navigation Menu
    nav_view = st.radio(
        "Navigation",
        [
            "🧹 Inbox Clean",
            "📦 Smart Bundles",
            "👥 Top Senders",
            "🛡️ Trash & Undo",
            "⚙️ Settings & Engine",
        ],
        index=0,
        label_visibility="collapsed",
        key="main_nav_radio"
    )

    st.markdown("---")

    # Minimal Background Worker Indicator
    status = worker.get_status()
    if status["is_running"]:
        st.info(f"⏳ **{status['task_name']}**")
        st.progress(status["progress_pct"] / 100.0)
        st.caption(f"{status['status_message']} ({status['elapsed_seconds']}s)")
        if st.button("Cancel", key="sb_cancel_worker", use_container_width=True):
            worker.request_cancel()
            st.toast("Cancellation requested!", icon="⚠️")
    else:
        st.caption(f"💾 **Local Database:** `{round(os.path.getsize(db_file) / (1024 * 1024), 1) if db_exists else 0} MB`")


# Load aggregate statistics
stats = db.get_stats()
total_emails = stats.get("total_emails", 0)
del_count = stats.get("pending_delete", 0)
review_count = stats.get("needs_review", 0)
keep_count = stats.get("kept", 0)
trashed_count = stats.get("trashed", 0)


# =============================================================================
# VIEW 1: 🧹 INBOX CLEAN (THE CENTRAL COCKPIT)
# =============================================================================
if nav_view == "🧹 Inbox Clean":
    # Hero Cleanliness Card
    with st.container():
        est_mb = round(del_count * 0.045, 1)
        cleanliness_pct = round((keep_count / max(1, total_emails - review_count)) * 100) if total_emails > 0 else 100

        st.markdown(f"""
        <div class="hero-container">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px;">
                <div>
                    <span class="chip chip-neutral" style="font-size: 0.85rem; margin-bottom: 8px;">
                        INBOX HEALTH: {cleanliness_pct}% CLEAN
                    </span>
                    <h1 style="margin: 8px 0 6px 0; font-size: 2.2rem; font-weight: 800; letter-spacing: -0.5px;">
                        {del_count:,} emails ready to clean
                    </h1>
                    <p style="margin: 0; font-size: 1.05rem; opacity: 0.75;">
                        Safely reclaim <b>~{est_mb} MB</b> of storage without touching receipts, tickets, or important threads.
                    </p>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Primary Action Row
    c_btn1, c_btn2, c_btn3 = st.columns([3, 2, 2])
    with c_btn1:
        if st.button(
            f"✨ Clean {del_count:,} Emails Now",
            type="primary",
            use_container_width=True,
            disabled=(status["is_running"] or del_count == 0),
            key="btn_hero_clean"
        ):
            def _task_hero_trash(run_id=None):
                return run_delete(
                    input_file=None,
                    dry_run=False,
                    email_addr=target_account,
                    run_id=run_id,
                    statuses=["CONFIDENT_DELETE", "PROBABLE_DELETE", "MANUAL_DELETE"]
                )

            started = worker.start_task(
                f"Clean Inbox ({del_count:,} emails)",
                _task_hero_trash,
                account=target_account,
                run_type="Clean Execution",
                run_params={"target_count": del_count}
            )
            if started:
                st.toast("Cleaning started in background!", icon="✨")
                st.rerun()

    with c_btn2:
        if st.button("🔄 Scan Latest Emails", use_container_width=True, disabled=status["is_running"], key="btn_hero_scan"):
            started = worker.start_task(
                "Scan Latest Emails (250)",
                run_streaming_pipeline,
                limit=250,
                batch_size=1,
                workers=DEFAULT_MAX_WORKERS,
                tier="paid",
                email_addr=target_account,
                account=target_account,
                run_type="Streaming Pipeline",
                run_params={"limit": 250, "batch_size": 1, "workers": DEFAULT_MAX_WORKERS, "tier": "paid"},
            )
            if started:
                st.toast("Scan launched in background!", icon="🔄")
                st.rerun()

    with c_btn3:
        if trashed_count > 0:
            if st.button(f"↩️ Undo Last Clean ({trashed_count:,})", use_container_width=True, disabled=status["is_running"], key="btn_hero_undo"):
                def _task_undo(run_id=None):
                    inp = get_latest_artifact("5_processed", target_account)
                    return run_restore(input_file=inp, dry_run=False, email_addr=target_account, run_id=run_id)

                started = worker.start_task("Restore Emails to Inbox", _task_undo, account=target_account, run_type="Restore Execution")
                if started:
                    st.toast("Restoration started!", icon="↩️")
                    st.rerun()

    st.markdown("<div style='margin: 20px 0;'></div>", unsafe_allow_html=True)

    # 3 Calm Status Chips
    s1, s2, s3 = st.columns(3)
    with s1:
        st.markdown(f"""
        <div class="clean-card" style="border-left: 4px solid #10b981;">
            <div style="font-size: 0.85rem; opacity: 0.75; font-weight: 600;">SAFE & PROTECTED</div>
            <div style="font-size: 1.6rem; font-weight: 800; margin: 4px 0;">{keep_count:,}</div>
            <div style="font-size: 0.82rem; opacity: 0.7;">Taxes, bank slips, tickets, and thread replies.</div>
        </div>
        """, unsafe_allow_html=True)

    with s2:
        st.markdown(f"""
        <div class="clean-card" style="border-left: 4px solid #f59e0b;">
            <div style="font-size: 0.85rem; opacity: 0.75; font-weight: 600;">NEEDS HUMAN REVIEW</div>
            <div style="font-size: 1.6rem; font-weight: 800; margin: 4px 0;">{review_count:,}</div>
            <div style="font-size: 0.82rem; opacity: 0.7;">Ambiguous notices flagged by Safety Auditor.</div>
        </div>
        """, unsafe_allow_html=True)

    with s3:
        st.markdown(f"""
        <div class="clean-card" style="border-left: 4px solid #ef4444;">
            <div style="font-size: 0.85rem; opacity: 0.75; font-weight: 600;">READY TO TRASH</div>
            <div style="font-size: 1.6rem; font-weight: 800; margin: 4px 0;">{del_count:,}</div>
            <div style="font-size: 0.82rem; opacity: 0.7;">Verified marketing, promotional blasts, and OTPs.</div>
        </div>
        """, unsafe_allow_html=True)

    # Contextual Quick Action if items need review
    if review_count > 0:
        st.markdown("<div style='margin: 16px 0;'></div>", unsafe_allow_html=True)
        with st.container():
            st.markdown(f"""
            <div class="clean-card" style="display: flex; justify-content: space-between; align-items: center; border-left: 4px solid #f59e0b;">
                <div>
                    <b>⚡ {review_count:,} emails are waiting for your review.</b>
                    <div style="font-size: 0.85rem; opacity: 0.75; margin-top: 2px;">
                        Quickly resolve all ambiguous emails with one click or explore them in Smart Bundles.
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            r_c1, r_c2 = st.columns(2)
            with r_c1:
                if st.button(f"🛡️ Keep All {review_count:,} Review Items", use_container_width=True, key="btn_quick_keep_all"):
                    cnt = db.resolve_all_needs_review("KEEP")
                    st.toast(f"Marked {cnt:,} items as KEEP!", icon="🛡️")
                    time.sleep(0.3)
                    st.rerun()
            with r_c2:
                if st.button(f"🗑️ Delete All {review_count:,} Review Items", use_container_width=True, key="btn_quick_del_all"):
                    cnt = db.resolve_all_needs_review("DELETE")
                    st.toast(f"Marked {cnt:,} items as DELETE!", icon="🗑️")
                    time.sleep(0.3)
                    st.rerun()

    # Auto-rerun loop if task is active
    if status["is_running"]:
        time.sleep(1.5)
        st.rerun()


# =============================================================================
# VIEW 2: 📦 SMART BUNDLES (CATEGORICAL CLEANING)
# =============================================================================
elif nav_view == "📦 Smart Bundles":
    st.markdown("## 📦 Smart Bundles")
    st.caption("Review and clean large volumes of email organized by natural human categories.")

    bundles = db.get_smart_bundles()

    # Render clean 2-column card grid
    col_a, col_b = st.columns(2)
    for idx, b in enumerate(bundles):
        target_col = col_a if idx % 2 == 0 else col_b
        with target_col:
            bundle_id = b["id"]
            tot = b["total"]
            del_c = b["delete_count"]
            keep_c = b["keep_count"]
            rev_c = b["review_count"]
            is_safe = b["safe_to_clean"]

            with st.container():
                st.markdown(f"""
                <div class="clean-card">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div>
                            <span style="font-size: 1.4rem;">{b['icon']}</span>
                            <span style="font-size: 1.1rem; font-weight: 700; margin-left: 6px;">{b['name']}</span>
                            <div style="font-size: 0.85rem; opacity: 0.7; margin: 4px 0 8px 0;">{b['description']}</div>
                        </div>
                        <span class="chip chip-neutral" style="font-weight: 700;">{tot:,} emails</span>
                    </div>
                    <div style="margin: 8px 0;">
                        <span class="chip chip-del">{del_c:,} To Clean</span> &nbsp;
                        <span class="chip chip-keep">{keep_c:,} Kept</span> &nbsp;
                        <span class="chip chip-review">{rev_c:,} Review</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                b_act1, b_act2 = st.columns(2)
                with b_act1:
                    if is_safe and tot > 0:
                        if st.button(f"🗑️ Clean Bundle ({tot:,})", key=f"btn_clean_bundle_{bundle_id}", use_container_width=True):
                            upd = db.bulk_override_by_bundle(bundle_id, "DELETE")
                            st.toast(f"Marked {upd:,} emails in '{b['name']}' as DELETE!", icon="🗑️")
                            time.sleep(0.3)
                            st.rerun()
                    else:
                        st.caption("🔒 Auto-protected category" if not is_safe else "Empty bundle")
                with b_act2:
                    if tot > 0:
                        if st.button(f"🛡️ Keep All ({tot:,})", key=f"btn_keep_bundle_{bundle_id}", use_container_width=True):
                            upd = db.bulk_override_by_bundle(bundle_id, "KEEP")
                            st.toast(f"Marked {upd:,} emails in '{b['name']}' as KEEP!", icon="🛡️")
                            time.sleep(0.3)
                            st.rerun()

                st.markdown("<div style='margin-bottom: 12px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 3: 👥 TOP SENDERS (HIGH-LEVERAGE CLUSTERS)
# =============================================================================
elif nav_view == "👥 Top Senders":
    st.markdown("## 👥 Top Senders")
    st.caption("Clean hundreds of repetitive marketing emails in a single click.")

    sender_search = st.text_input("Filter Senders", placeholder="Search sender address or domain...", key="top_senders_search")
    clusters = db.get_sender_clusters(limit=35, search=sender_search)

    if not clusters:
        st.info("No sender clusters found matching your query.")
    else:
        for idx, c in enumerate(clusters):
            sender_str = c["sender"]
            tot = c["total"]
            del_c = c["delete_count"]
            keep_c = c["keep_count"]
            rev_c = c["review_count"]

            # Compute initial letter for avatar
            clean_name = sender_str.split("<")[0].strip().strip('"') if "<" in sender_str else sender_str
            avatar_char = clean_name[0].upper() if clean_name else "M"

            with st.container():
                st.markdown(f"""
                <div class="clean-card">
                    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px;">
                        <div style="display: flex; align-items: center; gap: 12px;">
                            <div class="sender-avatar">{avatar_char}</div>
                            <div>
                                <div style="font-weight: 700; font-size: 1.02rem;">{sender_str}</div>
                                <div style="font-size: 0.82rem; opacity: 0.7;">Category: <b>{c['top_category']}</b></div>
                            </div>
                        </div>
                        <div>
                            <span class="chip chip-del">{del_c:,} Del</span>&nbsp;
                            <span class="chip chip-keep">{keep_c:,} Keep</span>&nbsp;
                            <span class="chip chip-review">{rev_c:,} Review</span>&nbsp;
                            <span class="chip chip-neutral"><b>{tot:,}</b> total</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                s_c1, s_c2, s_c3 = st.columns([5, 2, 2])
                with s_c1:
                    sample_txt = " • ".join(c["sample_subjects"][:2]) if c["sample_subjects"] else "No preview available"
                    st.caption(f"**Sample:** {sample_txt}")
                with s_c2:
                    if st.button(f"🗑️ Trash All ({tot:,})", key=f"btn_del_cluster_{idx}", use_container_width=True):
                        upd = db.bulk_override_by_sender(sender_str, "DELETE")
                        st.toast(f"Marked {upd:,} emails from '{sender_str}' as DELETE!", icon="🗑️")
                        time.sleep(0.3)
                        st.rerun()
                with s_c3:
                    if st.button(f"🛡️ Keep All ({tot:,})", key=f"btn_keep_cluster_{idx}", use_container_width=True):
                        upd = db.bulk_override_by_sender(sender_str, "KEEP")
                        st.toast(f"Marked {upd:,} emails from '{sender_str}' as KEEP!", icon="🛡️")
                        time.sleep(0.3)
                        st.rerun()

                st.markdown("<div style='margin-bottom: 8px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 4: 🛡️ TRASH & UNDO (PEACE OF MIND)
# =============================================================================
elif nav_view == "🛡️ Trash & Undo":
    st.markdown("## 🛡️ Trash & Undo")
    st.caption("Your safety net: emails are never permanently expunged until Gmail's 30-day Trash policy expires.")

    with st.container():
        st.markdown("""
        <div class="clean-card" style="border-left: 4px solid #3b82f6;">
            <b>🔒 30-Day Recovery Guarantee</b>
            <div style="font-size: 0.85rem; opacity: 0.8; margin-top: 4px;">
                Emails moved to Trash remain safely recoverable for 30 days. Clicking restore instantly moves them back to your Inbox by matching permanent RFC 822 Message-IDs.
            </div>
        </div>
        """, unsafe_allow_html=True)

    t_col1, t_col2 = st.columns([3, 2])
    with t_col1:
        st.markdown(f"### Currently Trashed: **{trashed_count:,} emails**")
    with t_col2:
        if st.button("↩️ Restore All Trashed Emails to Inbox", type="primary", disabled=(status["is_running"] or trashed_count == 0), use_container_width=True, key="btn_vault_restore_full"):
            def _task_restore_full(run_id=None):
                inp = get_latest_artifact("5_processed", target_account)
                return run_restore(input_file=inp, dry_run=False, email_addr=target_account, run_id=run_id)

            started = worker.start_task("Restore Emails to Inbox", _task_restore_full, account=target_account, run_type="Restore Execution")
            if started:
                st.toast("Restoration launched!", icon="↩️")
                st.rerun()

    st.markdown("---")

    # Preview of Trashed Emails
    trashed_rows, _ = db.get_emails_page(status_filter="TRASHED", limit=50, offset=0)
    if not trashed_rows:
        st.info("No emails are currently recorded as trashed in this account.")
    else:
        st.markdown("#### Recently Trashed Messages")
        trash_table = []
        for r in trashed_rows:
            trash_table.append({
                "UID": r["uid"],
                "Sender": r.get("sender", ""),
                "Subject": r.get("subject", ""),
                "Date": r.get("date", ""),
                "Reason": r.get("ai_reason", "") or r.get("validator_reason", "Cleaned"),
            })
        st.dataframe(pd.DataFrame(trash_table), use_container_width=True, hide_index=True)


# =============================================================================
# VIEW 5: ⚙️ SETTINGS & ENGINE TUNING
# =============================================================================
elif nav_view == "⚙️ Settings & Engine":
    st.markdown("## ⚙️ Settings & Engine Tuning")
    st.caption("Manage email credentials, adjust AI quotas, inspect logs, and perform database maintenance.")

    s_tab1, s_tab2, s_tab3, s_tab4 = st.tabs([
        "👥 Accounts",
        "⚡ Engine & AI Quotas",
        "📜 System Logs",
        "💾 Database Maintenance",
    ])

    with s_tab1:
        st.markdown("### Configured Inboxes")
        acc_list = db.list_accounts()
        if acc_list:
            acc_df = []
            for a in acc_list:
                acc_df.append({
                    "Email": a["email"],
                    "Display Name": a.get("display_name", ""),
                    "Default": "★ Default" if a.get("is_default") else "",
                    "Scanned": a.get("total_scanned", 0),
                    "Last Active": a.get("last_fetched_at", "-"),
                })
            st.dataframe(pd.DataFrame(acc_df), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### ➕ Add New Gmail Account")
        with st.form("form_add_acc_settings", clear_on_submit=False):
            new_em = st.text_input("Gmail Address", placeholder="user@gmail.com")
            new_pw = st.text_input("16-character App Password", type="password", help="From https://myaccount.google.com/apppasswords")
            new_lbl = st.text_input("Display Name", placeholder="e.g. Work, Personal")
            new_def = st.checkbox("Set as Default Account", value=False)
            if st.form_submit_button("Verify & Connect", type="primary"):
                if new_em and new_pw:
                    with st.spinner("Testing IMAP connection via SSL..."):
                        ok, msg = test_imap_credentials(new_em, new_pw)
                    if ok:
                        db.add_or_update_account(new_em, new_pw, new_lbl, is_default=new_def)
                        st.session_state["target_account"] = new_em.strip().lower()
                        st.success(f"Connected {new_em}!")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(f"Connection Failed: {msg}")

    with s_tab2:
        st.markdown("### Background Engine Tuning")
        st.caption("Tune parallelism and API rate limit quotas.")
        t_col1, t_col2 = st.columns(2)
        with t_col1:
            workers_val = st.slider("Gemini Concurrency Workers", min_value=1, max_value=20, value=DEFAULT_MAX_WORKERS)
        with t_col2:
            tier_val = st.selectbox("Google AI Quota Tier", ["paid", "free"], index=0, help="Paid tier allows 1,000 RPM; free tier is capped at 15 RPM.")
        st.info(f"Active Model: **gemini-2.5-flash** | Concurrency: **{workers_val} workers** | Quota Tier: **{tier_val}**")

    with s_tab3:
        st.markdown("### System Logs (`cleaner.log`)")
        log_lines = worker.tail_logs(target_account, lines=60)
        st.code("".join(log_lines) if log_lines else "(No logs recorded)", language="log")
        if st.button("🔄 Refresh Logs", key="btn_refresh_logs_settings"):
            st.rerun()

    with s_tab4:
        st.markdown("### Database Utilities & Reset")
        db_col1, db_col2 = st.columns(2)
        with db_col1:
            st.markdown("**📤 Export Database to CSV**")
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            exp_path = st.text_input("CSV Path", value=f"db_export_{ts}.csv", key="settings_exp_path")
            if st.button("Export CSV", key="btn_settings_export"):
                cnt = db.export_to_csv(exp_path)
                st.success(f"Exported {cnt:,} emails to `{exp_path}`")

        with db_col2:
            st.markdown("**📥 Backfill Missing Snippets**")
            if st.button("Backfill 500 Snippets", disabled=status["is_running"], key="btn_settings_backfill"):
                def _task_bf(run_id=None):
                    return db.backfill_missing_snippets(batch_size=100, limit=500)
                started = worker.start_task("Backfill Snippets", _task_bf, account=target_account, run_type="Snippet Backfill")
                if started:
                    st.toast("Backfill started in background!", icon="📥")
                    st.rerun()

        st.markdown("---")
        st.markdown("#### ⚠️ Reset Database")
        st.caption("Wipes local SQLite records and resets the fetch cursor back to 0.")
        c_wipe = st.checkbox("I confirm I want to wipe all local records and start fresh", value=False, key="chk_settings_wipe")
        if st.button("🗑️ Reset Database", disabled=(not c_wipe or status["is_running"]), type="primary", key="btn_settings_wipe_exec"):
            with st.spinner("Wiping local database..."):
                db.reset_database(reset_cursor=True)
                st.toast("Database cleaned and reset!", icon="✨")
                time.sleep(0.4)
                st.rerun()
