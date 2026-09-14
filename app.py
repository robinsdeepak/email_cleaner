"""
CleanMail - Production-Grade AI Gmail Cleaner.
Inspired by Clean Email, Superhuman, and Linear.

Core Views:
1. 🧹 Inbox Clean (Home Cockpit - Cleanliness Score & 1-Click Clean)
2. 📬 Email Review (Individual Email Triage: Mark/Approve/Reject with Snippets)
3. 📦 Smart Bundles (Categorical Cleaning with Group & Individual Controls)
4. 👥 Top Senders (Sender Clusters with Group & Individual Controls)
5. 🛡️ Trash & Undo (Individual & Group Restore by Sender/Category/All)
6. ⚙️ Settings & Engine (Accounts, AI Tuning, DB Tools, Logs)
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
        padding: 18px 22px;
        margin-bottom: 12px;
        border: 1px solid rgba(128, 128, 128, 0.14);
        background: rgba(128, 128, 128, 0.03);
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.02);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }
    .clean-card:hover {
        border-color: rgba(99, 102, 241, 0.4);
        box-shadow: 0 4px 14px rgba(0, 0, 0, 0.04);
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
        font-size: 0.90rem;
        padding: 5px 14px;
        transition: all 0.15s ease;
    }
    
    /* Layout Max Width */
    .block-container {
        padding-top: 1.8rem !important;
        padding-bottom: 3rem !important;
        max-width: 1140px !important;
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

    st.markdown("<div style='margin: 10px 0;'></div>", unsafe_allow_html=True)

    # 6-View Navigation Menu
    nav_view = st.radio(
        "Navigation",
        [
            "🧹 Inbox Clean",
            "📬 Email Review",
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

    # Background Worker Indicator
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

    # 3 Status Chips
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
                        Quickly resolve all ambiguous emails with one click or explore them individually in <b>📬 Email Review</b>.
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

    if status["is_running"]:
        time.sleep(1.5)
        st.rerun()


# =============================================================================
# VIEW 2: 📬 EMAIL REVIEW (INDIVIDUAL EMAIL CONTROLS)
# =============================================================================
elif nav_view == "📬 Email Review":
    st.markdown("## 📬 Email Review & Individual Triage")
    st.caption("Inspect individual email snippets and decisions. Mark, approve, or reject any email with one click.")

    # Search and Filter Toolbar
    f_col1, f_col2, f_col3, f_col4 = st.columns([3, 2, 2, 1])
    with f_col1:
        search_query = st.text_input("🔎 Search", placeholder="Sender, subject, UID...", key="review_search_input")
    with f_col2:
        action_filter = st.selectbox("Action Filter", ["ALL", "REVIEW", "DELETE", "KEEP"], key="review_action_filter")
    with f_col3:
        cat_filter = st.selectbox("Category", ["ALL", "MARKETING", "FOOD_TRANSIT", "JOB_ALERT", "SECURITY_OTP", "FINANCIAL", "INVOICE", "TRAVEL", "PERSONAL", "OTHER"], key="review_cat_filter")
    with f_col4:
        page_size = st.selectbox("Per Page", [25, 50, 100], index=0, key="review_page_size")

    if "rev_page" not in st.session_state:
        st.session_state.rev_page = 1

    rev_offset = (st.session_state.rev_page - 1) * page_size
    emails_list, total_matches = db.get_emails_page(
        search=search_query,
        action_filter=action_filter,
        category_filter=cat_filter,
        limit=page_size,
        offset=rev_offset
    )

    total_pages = max(1, (total_matches + page_size - 1) // page_size)

    # Top Pagination Bar
    p_c1, p_c2, p_c3 = st.columns([2, 4, 2])
    with p_c1:
        if st.button("⬅️ Previous", disabled=(st.session_state.rev_page <= 1), key="btn_rev_prev"):
            st.session_state.rev_page -= 1
            st.rerun()
    with p_c2:
        st.markdown(
            f"<div style='text-align: center; padding-top: 6px; font-weight: 600; font-size: 0.9rem;'>"
            f"Page {st.session_state.rev_page} of {total_pages} &nbsp;|&nbsp; {total_matches:,} emails"
            f"</div>",
            unsafe_allow_html=True
        )
    with p_c3:
        if st.button("Next ➡️", disabled=(st.session_state.rev_page >= total_pages), key="btn_rev_next"):
            st.session_state.rev_page += 1
            st.rerun()

    st.markdown("<div style='margin-bottom: 12px;'></div>", unsafe_allow_html=True)

    if not emails_list:
        st.info("No emails match your filter criteria.")
    else:
        for idx, em in enumerate(emails_list):
            uid = em["uid"]
            act = (em.get("final_action") or "REVIEW").upper()
            sender_txt = em.get("sender") or "Unknown"
            subj_txt = em.get("subject") or "(No Subject)"
            date_txt = em.get("date") or ""
            cat_txt = em.get("ai_category") or "OTHER"
            conf_txt = em.get("ai_confidence") or "MEDIUM"

            if act == "DELETE":
                chip_style = "chip-del"
                border_color = "#ef4444"
            elif act == "KEEP":
                chip_style = "chip-keep"
                border_color = "#10b981"
            else:
                chip_style = "chip-review"
                border_color = "#f59e0b"

            with st.container():
                st.markdown(f"""
                <div class="clean-card" style="border-left: 4px solid {border_color};">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 12px;">
                        <div style="flex: 1;">
                            <div style="font-size: 0.82rem; opacity: 0.7; margin-bottom: 3px;">
                                <b>#{uid}</b> &nbsp;•&nbsp; <b>{sender_txt}</b> &nbsp;•&nbsp; <code>{date_txt}</code>
                            </div>
                            <div style="font-size: 0.98rem; font-weight: 700; margin-bottom: 6px;">
                                {subj_txt}
                            </div>
                            <div>
                                <span class="chip chip-neutral">{cat_txt}</span> &nbsp;
                                <span class="chip chip-neutral">{conf_txt} Confidence</span>
                            </div>
                        </div>
                        <div style="text-align: right;">
                            <span class="chip {chip_style}">{act}</span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                act_c1, act_c2, act_c3, act_c4 = st.columns([5, 2, 2, 2])
                with act_c1:
                    with st.expander("🔍 Reason & Preview", expanded=False):
                        st.markdown(f"**AI Reason:** {em.get('ai_reason') or 'Unclassified'}")
                        if em.get("validator_reason"):
                            st.markdown(f"**Auditor Note:** {em.get('validator_reason')}")
                        st.code(em.get("snippet") or "(No snippet available)", language=None)
                with act_c2:
                    if st.button("🛡️ Keep", key=f"btn_indiv_keep_{uid}", use_container_width=True):
                        db.set_manual_override(uid, "KEEP", note="Individual review: keep")
                        st.toast(f"Marked #{uid} as KEEP!", icon="🛡️")
                        time.sleep(0.2)
                        st.rerun()
                with act_c3:
                    if st.button("🗑️ Delete", key=f"btn_indiv_del_{uid}", use_container_width=True):
                        db.set_manual_override(uid, "DELETE", note="Individual review: delete")
                        st.toast(f"Marked #{uid} as DELETE!", icon="🗑️")
                        time.sleep(0.2)
                        st.rerun()
                with act_c4:
                    if st.button("🟡 Review", key=f"btn_indiv_rev_{uid}", use_container_width=True):
                        db.set_manual_override(uid, "REVIEW", note="Individual review: flag review")
                        st.toast(f"Marked #{uid} as REVIEW!", icon="🟡")
                        time.sleep(0.2)
                        st.rerun()

                st.markdown("<div style='margin-bottom: 8px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 3: 📦 SMART BUNDLES (GROUP + INDIVIDUAL CONTROLS)
# =============================================================================
elif nav_view == "📦 Smart Bundles":
    st.markdown("## 📦 Smart Bundles")
    st.caption("Review and clean email categories as a group, or expand any bundle to triage individual messages.")

    bundles = db.get_smart_bundles()

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

                # Group Actions
                b_act1, b_act2 = st.columns(2)
                with b_act1:
                    if is_safe and tot > 0:
                        if st.button(f"🗑️ Clean Bundle ({tot:,})", key=f"btn_clean_bundle_{bundle_id}", use_container_width=True):
                            upd = db.bulk_override_by_bundle(bundle_id, "DELETE")
                            st.toast(f"Marked {upd:,} emails in '{b['name']}' as DELETE!", icon="🗑️")
                            time.sleep(0.3)
                            st.rerun()
                    else:
                        st.caption("🔒 Auto-protected" if not is_safe else "Empty bundle")
                with b_act2:
                    if tot > 0:
                        if st.button(f"🛡️ Keep Bundle ({tot:,})", key=f"btn_keep_bundle_{bundle_id}", use_container_width=True):
                            upd = db.bulk_override_by_bundle(bundle_id, "KEEP")
                            st.toast(f"Marked {upd:,} emails in '{b['name']}' as KEEP!", icon="🛡️")
                            time.sleep(0.3)
                            st.rerun()

                # Individual Email Inspector inside Bundle
                if tot > 0:
                    with st.expander(f"🔍 Inspect & Triage Emails in {b['name']}", expanded=False):
                        cats = b["categories"]
                        placeholders = ",".join("?" for _ in cats)
                        b_emails = db.query(f"""
                            SELECT uid, sender, subject, date, final_action
                            FROM emails
                            WHERE account = ? AND ai_category IN ({placeholders}) AND status != 'TRASHED'
                            LIMIT 15
                        """, (target_account, *cats))

                        for be in b_emails:
                            b_uid = be["uid"]
                            b_act = (be.get("final_action") or "REVIEW").upper()
                            c_sub1, c_sub2, c_sub3 = st.columns([5, 2, 2])
                            with c_sub1:
                                be_subj = be.get('subject', '')[:40]
                                be_sndr = be.get('sender', '')[:25]
                                st.caption(f"**#{b_uid}** | {be_sndr} | `{b_act}` — {be_subj}")
                            with c_sub2:
                                if st.button("🛡️ Keep", key=f"b_keep_{bundle_id}_{b_uid}", use_container_width=True):
                                    db.set_manual_override(b_uid, "KEEP")
                                    st.rerun()
                            with c_sub3:
                                if st.button("🗑️ Del", key=f"b_del_{bundle_id}_{b_uid}", use_container_width=True):
                                    db.set_manual_override(b_uid, "DELETE")
                                    st.rerun()

                st.markdown("<div style='margin-bottom: 12px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 4: 👥 TOP SENDERS (GROUP + INDIVIDUAL CONTROLS)
# =============================================================================
elif nav_view == "👥 Top Senders":
    st.markdown("## 👥 Top Senders")
    st.caption("Clean or protect high-volume senders in one click, or inspect and triage individual messages from each sender.")

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

                # Group Controls
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

                # Individual Controls per Sender
                with st.expander(f"🔍 Inspect & Triage Individual Emails from {clean_name[:30]}", expanded=False):
                    s_emails = db.query("""
                        SELECT uid, subject, date, final_action
                        FROM emails
                        WHERE account = ? AND sender = ? AND status != 'TRASHED'
                        LIMIT 15
                    """, (target_account, sender_str))

                    for se in s_emails:
                        s_uid = se["uid"]
                        s_act = (se.get("final_action") or "REVIEW").upper()
                        col_e1, col_e2, col_e3 = st.columns([5, 2, 2])
                        with col_e1:
                            se_subj = se.get('subject', '')[:45]
                            se_dt = se.get('date', '')
                            st.caption(f"**#{s_uid}** | `{s_act}` | {se_dt} — {se_subj}")
                        with col_e2:
                            if st.button("🛡️ Keep", key=f"s_keep_{idx}_{s_uid}", use_container_width=True):
                                db.set_manual_override(s_uid, "KEEP")
                                st.rerun()
                        with col_e3:
                            if st.button("🗑️ Del", key=f"s_del_{idx}_{s_uid}", use_container_width=True):
                                db.set_manual_override(s_uid, "DELETE")
                                st.rerun()

                st.markdown("<div style='margin-bottom: 8px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 5: 🛡️ TRASH & UNDO (INDIVIDUAL & GROUP RESTORATION)
# =============================================================================
elif nav_view == "🛡️ Trash & Undo":
    st.markdown("## 🛡️ Trash & Undo")
    st.caption("Safely recover deleted emails individually, by sender, by category, or all at once.")

    with st.container():
        st.markdown("""
        <div class="clean-card" style="border-left: 4px solid #3b82f6;">
            <b>🔒 30-Day Gmail Trash Retention</b>
            <div style="font-size: 0.85rem; opacity: 0.8; margin-top: 4px;">
                Emails moved to Trash remain safely in your Gmail Trash for 30 days before Google purges them.
                You can restore any email back to your Inbox individually or in batches by matching permanent Message-IDs.
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Master Restore All
    m_col1, m_col2 = st.columns([3, 2])
    with m_col1:
        st.markdown(f"### Currently Trashed: **{trashed_count:,} emails**")
    with m_col2:
        if st.button("↩️ Restore All Trashed Emails to Inbox", type="primary", disabled=(status["is_running"] or trashed_count == 0), use_container_width=True, key="btn_vault_restore_full"):
            def _task_restore_full(run_id=None):
                inp = get_latest_artifact("5_processed", target_account)
                return run_restore(input_file=inp, dry_run=False, email_addr=target_account, run_id=run_id)

            started = worker.start_task("Restore All Emails", _task_restore_full, account=target_account, run_type="Restore Execution")
            if started:
                st.toast("Restoration launched!", icon="↩️")
                st.rerun()

    st.markdown("---")

    # Group Controls for Restoration: By Sender & By Category
    if trashed_count > 0:
        st.markdown("### 👥 Group Restoration Controls")
        st.caption("Restore specific batches from Trash without un-trashing everything.")

        grp_c1, grp_c2 = st.columns(2)
        with grp_c1:
            st.markdown("**Restore by Sender:**")
            trashed_senders = db.get_trashed_senders(limit=25)
            if trashed_senders:
                sender_options = {s["sender"]: f"{s['sender']} ({s['count']} emails)" for s in trashed_senders}
                selected_restore_sender = st.selectbox(
                    "Select Sender to Restore",
                    options=list(sender_options.keys()),
                    format_func=lambda x: sender_options.get(x, x),
                    key="sb_restore_sender"
                )
                if st.button("↩️ Restore Sender to Inbox", key="btn_restore_sender_action", use_container_width=True):
                    # Get UIDs for this sender
                    t_uids = [r["uid"] for r in db.query("SELECT uid FROM emails WHERE account = ? AND sender = ? AND status = 'TRASHED'", (target_account, selected_restore_sender))]
                    if t_uids:
                        def _task_restore_sender(run_id=None):
                            return run_restore(uids=t_uids, email_addr=target_account, run_id=run_id)
                        started = worker.start_task(f"Restore Sender ({len(t_uids)} emails)", _task_restore_sender, account=target_account, run_type="Restore Sender")
                        if started:
                            st.toast(f"Restoring {len(t_uids)} emails from '{selected_restore_sender}'...", icon="↩️")
                            st.rerun()

        with grp_c2:
            st.markdown("**Restore by Category:**")
            trashed_cats = db.get_trashed_categories()
            if trashed_cats:
                cat_options = {c["category"]: f"{c['category']} ({c['count']} emails)" for c in trashed_cats}
                selected_restore_cat = st.selectbox(
                    "Select Category to Restore",
                    options=list(cat_options.keys()),
                    format_func=lambda x: cat_options.get(x, x),
                    key="sb_restore_cat"
                )
                if st.button("↩️ Restore Category to Inbox", key="btn_restore_cat_action", use_container_width=True):
                    t_uids = [r["uid"] for r in db.query("SELECT uid FROM emails WHERE account = ? AND ai_category = ? AND status = 'TRASHED'", (target_account, selected_restore_cat))]
                    if t_uids:
                        def _task_restore_cat(run_id=None):
                            return run_restore(uids=t_uids, email_addr=target_account, run_id=run_id)
                        started = worker.start_task(f"Restore Category ({len(t_uids)} emails)", _task_restore_cat, account=target_account, run_type="Restore Category")
                        if started:
                            st.toast(f"Restoring {len(t_uids)} emails from '{selected_restore_cat}'...", icon="↩️")
                            st.rerun()

        st.markdown("<div style='margin-bottom: 16px;'></div>", unsafe_allow_html=True)

    # Individual Trashed Email Control
    st.markdown("### 📬 Individual Trashed Emails")
    st.caption("Restore single messages directly back to your Gmail Inbox.")

    t_search = st.text_input("Filter Trashed Messages", placeholder="Search sender, subject, UID...", key="trash_filter_input")
    trashed_rows, t_matches = db.get_emails_page(search=t_search, status_filter="TRASHED", limit=30, offset=0)

    if not trashed_rows:
        st.info("No trashed emails match your query.")
    else:
        for tr in trashed_rows:
            t_uid = tr["uid"]
            t_sender = tr.get("sender", "Unknown")
            t_subj = tr.get("subject", "(No Subject)")
            t_date = tr.get("date", "")
            t_cat = tr.get("ai_category", "OTHER")

            with st.container():
                st.markdown(f"""
                <div class="clean-card" style="border-left: 4px solid #ef4444;">
                    <div style="display: flex; justify-content: space-between; align-items: center; gap: 12px;">
                        <div style="flex: 1;">
                            <div style="font-size: 0.82rem; opacity: 0.7; margin-bottom: 2px;">
                                <b>#{t_uid}</b> &nbsp;•&nbsp; <b>{t_sender}</b> &nbsp;•&nbsp; <code>{t_date}</code>
                            </div>
                            <div style="font-weight: 700; font-size: 0.98rem;">
                                {t_subj}
                            </div>
                            <div style="margin-top: 4px;">
                                <span class="chip chip-neutral">{t_cat}</span>
                            </div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                tr_c1, tr_c2 = st.columns([6, 2])
                with tr_c1:
                    st.caption(f"Reason: {tr.get('ai_reason') or tr.get('validator_reason') or 'Cleaned'}")
                with tr_c2:
                    if st.button("↩️ Restore", key=f"btn_restore_indiv_{t_uid}", use_container_width=True):
                        def _task_restore_single(run_id=None):
                            return run_restore(uids=[t_uid], email_addr=target_account, run_id=run_id)
                        started = worker.start_task(f"Restore Email #{t_uid}", _task_restore_single, account=target_account, run_type="Restore Single")
                        if started:
                            st.toast(f"Restoring #{t_uid}...", icon="↩️")
                            st.rerun()

                st.markdown("<div style='margin-bottom: 6px;'></div>", unsafe_allow_html=True)


# =============================================================================
# VIEW 6: ⚙️ SETTINGS & ENGINE TUNING
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
        st.markdown("### Database Maintenance & Reset")
        st.caption("Export records or wipe local database to start fresh.")

        st.markdown("**📤 Export Database to CSV**")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        exp_path = st.text_input("CSV Export Path", value=f"db_export_{ts}.csv", key="settings_exp_path")
        if st.button("Export to CSV", key="btn_settings_export"):
            cnt = db.export_to_csv(exp_path)
            st.success(f"Exported {cnt:,} emails to `{exp_path}`")

        st.markdown("---")
        st.markdown("#### ⚠️ Reset Database")
        st.caption("Permanently wipes all local SQLite records and resets the fetch cursor back to 0.")
        c_wipe = st.checkbox("I confirm I want to wipe all local records and start fresh", value=False, key="chk_settings_wipe")
        if st.button("🗑️ Reset Database", disabled=(not c_wipe or status["is_running"]), type="primary", key="btn_settings_wipe_exec"):
            with st.spinner("Wiping local database..."):
                db.reset_database(reset_cursor=True)
                st.toast("Database cleaned and reset!", icon="✨")
                time.sleep(0.4)
                st.rerun()
