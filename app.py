"""
CleanMail Studio - Intelligent Gmail AI Cleaner Web Dashboard.

A modern, database-driven email cleaning suite featuring:
- Executive Mission Control with real-time KPI health metrics and one-click smart clean.
- Smart Review Studio with 3 view modes: Sender Clusters, Interactive Grid, and Focus Cards.
- Safe Trash Vault with multi-status checklist, dry-run simulation, and 1-click restore.
- Complete multi-account management with live SSL onboarding and SQLite maintenance.
"""

import json
import os
import sys
import time
from datetime import datetime

# Ensure src/ is in sys.path
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
from gmail_cleaner.cli import run_all_pipeline
from gmail_cleaner.stages import run_delete, run_restore
from gmail_cleaner.streaming import run_streaming_pipeline

# -----------------------------------------------------------------------------
# PAGE CONFIGURATION & THEME STYLING
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="CleanMail Studio | Gmail AI Cleaner",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Theme-adaptive Card & Container tokens */
    .studio-card {
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 12px;
        border: 1px solid rgba(128, 128, 128, 0.2);
        background: rgba(128, 128, 128, 0.04);
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.04);
        transition: all 0.2s ease-in-out;
    }
    .studio-card:hover {
        border-color: rgba(128, 128, 128, 0.35);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    }
    
    /* Semantic Status Border Ribbons */
    .ribbon-delete {
        border-left: 5px solid #ef4444 !important;
    }
    .ribbon-keep {
        border-left: 5px solid #10b981 !important;
    }
    .ribbon-review {
        border-left: 5px solid #f59e0b !important;
    }
    .ribbon-protected {
        border-left: 5px solid #3b82f6 !important;
    }

    /* Semantic Status Badges */
    .badge-delete {
        background-color: rgba(239, 68, 68, 0.15);
        color: #ef4444;
        padding: 3px 9px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.82rem;
        letter-spacing: 0.3px;
        display: inline-block;
    }
    .badge-keep {
        background-color: rgba(16, 185, 129, 0.15);
        color: #10b981;
        padding: 3px 9px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.82rem;
        letter-spacing: 0.3px;
        display: inline-block;
    }
    .badge-review {
        background-color: rgba(245, 158, 11, 0.15);
        color: #f59e0b;
        padding: 3px 9px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.82rem;
        letter-spacing: 0.3px;
        display: inline-block;
    }
    .badge-category {
        background-color: rgba(59, 130, 246, 0.12);
        color: #3b82f6;
        padding: 2px 8px;
        border-radius: 5px;
        font-size: 0.76rem;
        font-weight: 600;
        letter-spacing: 0.4px;
        display: inline-block;
    }
    .badge-confidence {
        background-color: rgba(168, 85, 247, 0.12);
        color: #a855f7;
        padding: 2px 8px;
        border-radius: 5px;
        font-size: 0.76rem;
        font-weight: 600;
        letter-spacing: 0.4px;
        display: inline-block;
    }
    .badge-sender-count {
        background-color: rgba(128, 128, 128, 0.15);
        padding: 3px 9px;
        border-radius: 12px;
        font-weight: 700;
        font-size: 0.82rem;
    }

    /* Hero Metric Styling */
    .hero-stat {
        background: rgba(128, 128, 128, 0.05);
        border: 1px solid rgba(128, 128, 128, 0.18);
        border-radius: 10px;
        padding: 14px 16px;
        text-align: center;
    }

    /* Button Polish */
    .stButton>button {
        border-radius: 8px;
        font-weight: 600;
        transition: transform 0.1s ease;
    }
    .stButton>button:active {
        transform: scale(0.98);
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# SIDEBAR: Universal Context & Multi-Account Switcher
# -----------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 📧 CleanMail Studio")
    st.caption("AI-Powered Gmail Hygiene Suite")

    # Connect to root DB to list configured accounts
    temp_db = EmailDB()
    configured_accounts = temp_db.list_accounts()

    if not configured_accounts:
        target_account = st.text_input("Target Gmail Account", value=GMAIL_USER)
    else:
        acc_emails = [acc["email"] for acc in configured_accounts]

        def _format_acc(email_str):
            for a in configured_accounts:
                if a["email"] == email_str:
                    def_tag = " ★" if a.get("is_default") else ""
                    return f"{a.get('display_name', 'Account')}{def_tag} ({a['email']})"
            return email_str

        default_acc = next((a["email"] for a in configured_accounts if a.get("is_default")), acc_emails[0])
        current_selection = st.session_state.get("target_account", default_acc)
        if current_selection not in acc_emails:
            current_selection = default_acc

        selected_idx = acc_emails.index(current_selection) if current_selection in acc_emails else 0
        target_account = st.selectbox(
            "Active Gmail Account",
            options=acc_emails,
            index=selected_idx,
            format_func=_format_acc,
            key="sb_active_account_select",
            help="Select which Gmail account to view, audit, and clean."
        )
        st.session_state["target_account"] = target_account

    db = EmailDB(account=target_account)
    db_file = db.db_path
    db_exists = os.path.isfile(db_file)

    # ➕ Onboard Account Form
    with st.expander("➕ Onboard New Account", expanded=False):
        st.caption("Add another Gmail inbox. App Passwords are encrypted inside local SQLite (`emails.db`).")
        with st.form("form_onboard_account", clear_on_submit=False):
            new_email = st.text_input("Gmail Address", placeholder="user@gmail.com")
            new_pwd = st.text_input(
                "16-char App Password",
                type="password",
                placeholder="xxxx xxxx xxxx xxxx",
                help="Google 2-Step Verification App Password from https://myaccount.google.com/apppasswords"
            )
            new_name = st.text_input("Display Name", placeholder="e.g. Personal, Work")
            new_is_def = st.checkbox("Set as Default Account", value=False)
            btn_save_acc = st.form_submit_button("🧪 Verify & Connect", type="primary", use_container_width=True)

            if btn_save_acc:
                if not new_email or not new_pwd:
                    st.error("Please provide both email address and App Password.")
                else:
                    with st.spinner("Connecting to Gmail IMAP via SSL..."):
                        is_ok, msg = test_imap_credentials(new_email, new_pwd)
                    if is_ok:
                        db.add_or_update_account(new_email, new_pwd, new_name, is_default=new_is_def)
                        st.session_state["target_account"] = new_email.strip().lower()
                        st.success(f"Verified & Onboarded: {new_email}!")
                        st.toast(f"Account '{new_email}' saved!", icon="🎉")
                        time.sleep(0.6)
                        st.rerun()
                    else:
                        st.error(f"Connection Failed: {msg}")
                        st.markdown("[👉 Generate a Google App Password](https://myaccount.google.com/apppasswords)")

    st.markdown("---")
    
    # Quick Worker Telemetry
    st.markdown("#### ⚡ Pipeline Status")
    status = worker.get_status()
    if status["is_running"]:
        st.info(f"⚙️ **{status['task_name']}**")
        st.progress(status["progress_pct"] / 100.0)
        st.caption(f"{status['status_message']} ({status['elapsed_seconds']}s)")
        if st.button("🛑 Cancel Task", key="sidebar_cancel_btn", type="secondary", use_container_width=True):
            worker.request_cancel()
            st.toast("Cancellation requested!", icon="⚠️")
    else:
        latest_run = db.get_latest_run()
        if latest_run:
            run_status = (latest_run.get("status") or "COMPLETED").upper()
            if run_status == "COMPLETED":
                st.success(f"✅ Last: **{latest_run.get('action_type', 'Task')}**")
            elif run_status == "FAILED":
                st.error(f"❌ Last: **{latest_run.get('action_type', 'Task')}** (Failed)")
            elif run_status == "CANCELLED":
                st.warning(f"🛑 Last: **{latest_run.get('action_type', 'Task')}** (Cancelled)")
            else:
                st.info(f"ℹ️ Last: **{latest_run.get('action_type', 'Task')}**")

            st.caption(f"**Run:** `{latest_run['run_id'][:24]}...`")
            st.caption(f"📅 {latest_run.get('started_at', '')}")
            dur = latest_run.get("duration_seconds")
            if dur:
                st.caption(f"⏱️ **Duration:** {dur:.1f}s")
        else:
            st.success("🟢 Worker Idle")
            st.caption("No previous runs recorded.")

    st.markdown("---")
    st.caption(f"**Local Database:** `emails.db` (WAL Mode)")
    st.caption(f"**Storage Size:** {round(os.path.getsize(db_file) / (1024 * 1024), 2) if db_exists else 0} MB")


# -----------------------------------------------------------------------------
# WORKSPACE TABS: 4 ACTION-ORIENTED PILLARS
# -----------------------------------------------------------------------------
tab_mission, tab_review, tab_vault, tab_settings = st.tabs([
    "🎯 Mission Control",
    "🔍 Smart Review Studio",
    "🛡️ Trash Vault & Restore",
    "⚙️ Accounts & Maintenance",
])

stats = db.get_stats()
total_emails = stats.get("total_emails", 0)
del_count = stats.get("pending_delete", 0)
review_count = stats.get("needs_review", 0)
keep_count = stats.get("kept", 0)
trashed_count = stats.get("trashed", 0)


# =============================================================================
# TAB 1: MISSION CONTROL & PIPELINE ENGINE
# =============================================================================
with tab_mission:
    st.markdown("## 🎯 Mission Control")
    st.caption("Executive overview, health telemetry, and one-click smart pipeline execution.")

    # Executive Hero KPI Row
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    with kpi_col1:
        st.metric("Total Emails Tracked", f"{total_emails:,}", help="All emails fetched and cataloged in SQLite")
    with kpi_col2:
        del_pct = round((del_count / total_emails) * 100, 1) if total_emails > 0 else 0
        est_mb = round(del_count * 0.045, 1)
        st.metric("Pending Deletion", f"{del_count:,}", f"{del_pct}% of total (~{est_mb} MB)", delta_color="inverse")
    with kpi_col3:
        rev_pct = round((review_count / total_emails) * 100, 1) if total_emails > 0 else 0
        st.metric("🟡 Needs Human Review", f"{review_count:,}", f"{rev_pct}% of total")
    with kpi_col4:
        keep_pct = round((keep_count / total_emails) * 100, 1) if total_emails > 0 else 0
        st.metric("Safe & Protected", f"{keep_count:,}", f"{keep_pct}% of total")

    st.markdown("---")

    # Main Action Hub: 2 Balanced Columns
    hub_left, hub_right = st.columns([5, 4])

    with hub_left:
        st.markdown("### ⚡ Smart Clean Runner")
        st.caption("Fetches new emails from Gmail, triages with Gemini 2.5 Flash, runs Safety Auditor, and persists results.")

        # Preset Limit Chips
        limit_choice = st.radio(
            "Batch Scan Volume",
            [100, 250, 500, 1000, 2500],
            index=2,
            horizontal=True,
            key="smart_limit_radio",
            help="Select how many recent emails to scan and process."
        )

        cost_est = limit_choice * 0.000375
        st.info(f"💡 **Estimated Inference Cost:** **${cost_est:.3f}** (~₹{cost_est * 87:.1f}) for {limit_choice:,} emails via Gemini 2.5 Flash.")

        with st.expander("🛠️ Advanced Engine Tuning", expanded=False):
            st.caption("Configure concurrency and API quotas.")
            t_col1, t_col2 = st.columns(2)
            with t_col1:
                tuning_workers = st.slider("Worker Threads", min_value=1, max_value=20, value=DEFAULT_MAX_WORKERS, key="tune_workers")
            with t_col2:
                tuning_tier = st.selectbox("API Tier", ["paid", "free"], index=0, key="tune_tier")
            tuning_batch = st.slider("Batch Size", min_value=1, max_value=50, value=1, key="tune_batch", help="1 email/call guarantees zero batch hallucination; >1 groups requests.")

        if st.button("🚀 Launch Smart Clean Pipeline", type="primary", disabled=status["is_running"], use_container_width=True, key="btn_run_smart_clean"):
            started = worker.start_task(
                f"Smart Clean ({limit_choice} emails)",
                run_streaming_pipeline,
                limit=limit_choice,
                batch_size=tuning_batch,
                workers=tuning_workers,
                tier=tuning_tier,
                email_addr=target_account,
                account=target_account,
                run_type="Streaming Pipeline",
                run_params={"limit": limit_choice, "batch_size": tuning_batch, "workers": tuning_workers, "tier": tuning_tier},
            )
            if started:
                st.toast("Smart Clean Pipeline launched in background!", icon="🚀")
                st.rerun()

    with hub_right:
        st.markdown("### 🛡️ Safety & Hygiene Actions")
        st.caption("Maintain database cleanliness and re-evaluate classification rules.")

        # Re-Scan Kept Emails
        st.markdown("**🔄 Re-Scan Kept Emails**")
        st.caption("Re-evaluates emails currently marked as KEEP with updated classification prompts (0 IMAP calls).")
        if st.button("Re-Evaluate All Kept Emails", disabled=status["is_running"], use_container_width=True, key="btn_rescan_kept_hub"):
            def _task_rescan(run_id=None):
                db_inst = EmailDB(account=target_account)
                reset_count = db_inst.reset_kept_for_rescan()
                from gmail_cleaner.stages import run_scan, run_validate
                run_scan(input_file=None, workers=10, email_addr=target_account, only_kept=False, run_id=run_id)
                run_validate(input_file=None, workers=10, email_addr=target_account, run_id=run_id)
                if run_id:
                    stats_now = db_inst.get_stats()
                    db_inst.update_run(
                        run_id,
                        status="COMPLETED",
                        total_emails=reset_count,
                        delete_count=stats_now.get("pending_delete", 0),
                        keep_count=stats_now.get("kept", 0),
                        rescued_count=stats_now.get("needs_review", 0),
                    )
                return f"Re-scan complete: {reset_count} emails re-evaluated."

            started = worker.start_task("Re-Scan Kept Emails", _task_rescan, account=target_account, run_type="Re-Scan Kept")
            if started:
                st.toast("Re-Scan task launched!", icon="🔄")
                st.rerun()

        st.markdown("<hr style='margin: 12px 0;'/>", unsafe_allow_html=True)

        # Snippet Refill
        with db.get_connection() as conn:
            missing_snippets = conn.execute(
                "SELECT COUNT(*) FROM emails WHERE account = ? AND (snippet IS NULL OR snippet = '' OR snippet = '(No snippet available)') AND status != 'TRASHED'",
                (target_account,)
            ).fetchone()[0]

        st.markdown(f"**📥 Snippet Backfill ({missing_snippets:,} empty)**")
        st.caption("Fetches 10KB previews for emails missing snippets via safe IMAP.")
        if st.button("Backfill Missing Snippets", disabled=(status["is_running"] or missing_snippets == 0), use_container_width=True, key="btn_refill_snippets_hub"):
            def _task_backfill(run_id=None):
                db_inst = EmailDB(account=target_account)
                updated = db_inst.backfill_missing_snippets(batch_size=100, limit=500)
                if run_id:
                    db_inst.update_run(run_id, total_emails=updated, status="COMPLETED")
                return updated

            started = worker.start_task("Snippet Backfill", _task_backfill, account=target_account, run_type="Snippet Backfill")
            if started:
                st.toast("Snippet backfill started!", icon="📥")
                st.rerun()

    # Visual Analytics Row
    st.markdown("---")
    st.markdown("### 📈 Visual Distribution Analytics")
    v_col1, v_col2 = st.columns(2)
    with v_col1:
        st.markdown("**Decisions & Confidence Tiers**")
        chart_data = pd.DataFrame({
            "Decision Tier": ["Confident Delete", "Probable Delete", "Needs Review", "Confident Keep", "Already Trashed"],
            "Count": [
                stats.get("confident_delete", 0),
                stats.get("probable_delete", 0),
                stats.get("needs_review", 0),
                stats.get("confident_keep", 0),
                trashed_count,
            ]
        })
        st.bar_chart(chart_data.set_index("Decision Tier"))

    with v_col2:
        st.markdown("**Category Breakdown**")
        cat_dict = db.get_category_stats()
        if cat_dict:
            cat_df = pd.DataFrame(list(cat_dict.items()), columns=["Category", "Emails"])
            st.bar_chart(cat_df.set_index("Category"))
        else:
            st.caption("No category distribution available yet.")

    # Collapsible Terminal Drawer
    st.markdown("---")
    with st.expander("📜 Live Pipeline Terminal (cleaner.log)", expanded=status["is_running"]):
        log_lines = worker.tail_logs(target_account, lines=45)
        st.code("".join(log_lines) if log_lines else "(No active logs recorded)", language="log")
        if st.button("🔄 Refresh Terminal Logs", key="btn_refresh_logs_hub"):
            st.rerun()

    if status["is_running"]:
        time.sleep(1.5)
        st.rerun()


# =============================================================================
# TAB 2: SMART REVIEW STUDIO (3 VIEW MODES)
# =============================================================================
with tab_review:
    st.markdown("## 🔍 Smart Review Studio")
    st.caption("Triage and approve decisions using high-leverage sender clustering, fast spreadsheet editing, or visual focus cards.")

    # Batch Action Header
    if review_count > 0 or stats.get("confident_delete", 0) > 0:
        conf_del = stats.get("confident_delete", 0)
        with st.container():
            st.info(f"📋 **Action Items:** **{review_count:,}** emails pending review &nbsp;|&nbsp; **{conf_del:,}** confident deletions awaiting confirmation")
            b_col1, b_col2, b_col3 = st.columns(3)
            with b_col1:
                if review_count > 0:
                    if st.button(f"🛡️ Keep All Needs Review ({review_count:,})", use_container_width=True, key="btn_bulk_keep"):
                        cnt = db.resolve_all_needs_review("KEEP")
                        st.toast(f"Marked {cnt:,} items as KEEP!", icon="🛡️")
                        time.sleep(0.3)
                        st.rerun()
            with b_col2:
                if review_count > 0:
                    if st.button(f"🗑️ Delete All Needs Review ({review_count:,})", use_container_width=True, key="btn_bulk_del"):
                        cnt = db.resolve_all_needs_review("DELETE")
                        st.toast(f"Marked {cnt:,} items as DELETE!", icon="🗑️")
                        time.sleep(0.3)
                        st.rerun()
            with b_col3:
                if conf_del > 0:
                    if st.button(f"✅ Approve Confident Deletes ({conf_del:,})", use_container_width=True, key="btn_bulk_conf"):
                        cnt = db.approve_confident_deletions()
                        st.toast(f"Approved {cnt:,} confident deletions!", icon="✅")
                        time.sleep(0.3)
                        st.rerun()
            st.markdown("<hr style='margin: 8px 0 16px 0;'/>", unsafe_allow_html=True)

    # View Mode Switcher
    view_mode = st.radio(
        "Select Triage View Mode:",
        ["📊 Sender Clusters (Super-Fast Bulk Cleaning)", "📑 Interactive Grid (Spreadsheet Multi-Select)", "🎴 Visual Focus Cards (Detailed Inspection)"],
        index=0,
        horizontal=True,
        key="studio_view_mode"
    )

    st.markdown("---")

    # -------------------------------------------------------------------------
    # MODE 1: SENDER CLUSTERS
    # -------------------------------------------------------------------------
    if "Sender Clusters" in view_mode:
        st.markdown("### 📊 Top Sender Clusters")
        st.caption("Clean hundreds of repetitive emails with single-click sender-level actions.")

        s_search = st.text_input("Filter Senders", placeholder="Search sender address or domain...", key="sender_cluster_search")
        clusters = db.get_sender_clusters(limit=40, search=s_search)

        if not clusters:
            st.info("No sender clusters match your search.")
        else:
            for c_idx, c in enumerate(clusters):
                sender_name = c["sender"]
                tot = c["total"]
                del_c = c["delete_count"]
                keep_c = c["keep_count"]
                rev_c = c["review_count"]

                # Card with status-based border ribbon
                ribbon_cls = "ribbon-delete" if del_c > keep_c else ("ribbon-keep" if keep_c > del_c else "ribbon-review")

                with st.container():
                    st.markdown(f"""
                    <div class="studio-card {ribbon_cls}">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <div>
                                <span style="font-size: 1.05rem; font-weight: 700;">{sender_name}</span>
                                &nbsp;&nbsp;<span class="badge-sender-count">{tot:,} emails</span>
                                &nbsp;&nbsp;<span class="badge-category">{c['top_category']}</span>
                            </div>
                            <div>
                                <span class="badge-delete">{del_c:,} Del</span>&nbsp;
                                <span class="badge-keep">{keep_c:,} Keep</span>&nbsp;
                                <span class="badge-review">{rev_c:,} Review</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    btn_c1, btn_c2, btn_c3 = st.columns([5, 2, 2])
                    with btn_c1:
                        sample_str = " • ".join(c["sample_subjects"][:2]) if c["sample_subjects"] else "No subject preview"
                        st.caption(f"**Sample Subjects:** {sample_str}")
                    with btn_c2:
                        if st.button(f"🗑️ Trash All ({tot:,})", key=f"btn_trash_sender_{c_idx}", use_container_width=True):
                            upd = db.bulk_override_by_sender(sender_name, "DELETE")
                            st.toast(f"Marked all {upd:,} emails from '{sender_name}' as DELETE!", icon="🗑️")
                            time.sleep(0.3)
                            st.rerun()
                    with btn_c3:
                        if st.button(f"🛡️ Keep All ({tot:,})", key=f"btn_keep_sender_{c_idx}", use_container_width=True):
                            upd = db.bulk_override_by_sender(sender_name, "KEEP")
                            st.toast(f"Marked all {upd:,} emails from '{sender_name}' as KEEP!", icon="🛡️")
                            time.sleep(0.3)
                            st.rerun()

                    st.markdown("<div style='margin-bottom: 8px;'></div>", unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # MODE 2: INTERACTIVE GRID
    # -------------------------------------------------------------------------
    elif "Interactive Grid" in view_mode:
        st.markdown("### 📑 High-Density Interactive Grid")
        st.caption("Sort, filter, and batch-select emails. Edit actions directly in the grid without page reload delays.")

        # Grid Filter Toolbar
        g_col1, g_col2, g_col3 = st.columns([3, 2, 2])
        with g_col1:
            grid_search = st.text_input("🔎 Search Grid", placeholder="Sender, subject, UID...", key="grid_search_input")
        with g_col2:
            grid_action = st.selectbox("Action Filter", ["ALL", "REVIEW", "DELETE", "KEEP"], key="grid_action_filter")
        with g_col3:
            grid_page_size = st.selectbox("Rows per page", [50, 100, 200], index=0, key="grid_page_size")

        grid_rows, grid_total = db.get_emails_page(
            search=grid_search,
            action_filter=grid_action,
            limit=grid_page_size,
            offset=0
        )

        if not grid_rows:
            st.info("No emails match your grid filter.")
        else:
            # Prepare dataframe with selectable checkbox column
            grid_data = []
            for r in grid_rows:
                grid_data.append({
                    "Select": False,
                    "UID": r["uid"],
                    "Action": (r.get("final_action") or "REVIEW").upper(),
                    "Sender": r.get("sender", ""),
                    "Subject": r.get("subject", ""),
                    "Category": r.get("ai_category", "OTHER"),
                    "Confidence": r.get("ai_confidence", "MEDIUM"),
                    "AI Decision": r.get("ai_decision", ""),
                    "Date": r.get("date", ""),
                })
            df_grid = pd.DataFrame(grid_data)

            st.caption(f"Showing **{len(df_grid):,}** of **{grid_total:,}** matching emails.")

            edited_df = st.data_editor(
                df_grid,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Select": st.column_config.CheckboxColumn("Select", help="Check to apply batch action"),
                    "UID": st.column_config.NumberColumn("UID", width="small", disabled=True),
                    "Action": st.column_config.SelectboxColumn("Action", options=["DELETE", "KEEP", "REVIEW"], required=True, width="small"),
                    "Sender": st.column_config.TextColumn("Sender", width="medium", disabled=True),
                    "Subject": st.column_config.TextColumn("Subject", width="large", disabled=True),
                    "Category": st.column_config.TextColumn("Category", width="small", disabled=True),
                    "Confidence": st.column_config.TextColumn("Confidence", width="small", disabled=True),
                    "Date": st.column_config.TextColumn("Date", width="small", disabled=True),
                },
                disabled=["UID", "Sender", "Subject", "Category", "Confidence", "Date", "AI Decision"],
                key="studio_data_editor"
            )

            # Batch Action Bar for Checked Rows
            selected_rows = edited_df[edited_df["Select"] == True]
            num_selected = len(selected_rows)

            col_act1, col_act2, col_act3 = st.columns([3, 2, 2])
            with col_act1:
                st.write(f"**{num_selected:,}** emails selected with checkbox")
            with col_act2:
                if st.button("🛡️ Mark Selected as KEEP", disabled=(num_selected == 0), use_container_width=True, key="btn_grid_batch_keep"):
                    uids = selected_rows["UID"].tolist()
                    db.bulk_set_final_action(uids, "KEEP", note="Grid batch override: keep")
                    st.toast(f"Marked {len(uids):,} emails as KEEP!", icon="🛡️")
                    time.sleep(0.3)
                    st.rerun()
            with col_act3:
                if st.button("🗑️ Mark Selected as DELETE", disabled=(num_selected == 0), use_container_width=True, key="btn_grid_batch_del"):
                    uids = selected_rows["UID"].tolist()
                    db.bulk_set_final_action(uids, "DELETE", note="Grid batch override: delete")
                    st.toast(f"Marked {len(uids):,} emails as DELETE!", icon="🗑️")
                    time.sleep(0.3)
                    st.rerun()

            # Check if any inline Action dropdowns were modified
            changes = []
            for i, row in edited_df.iterrows():
                orig_action = df_grid.loc[i, "Action"]
                new_action = row["Action"]
                if orig_action != new_action:
                    changes.append((row["UID"], new_action))

            if changes:
                if st.button(f"💾 Save {len(changes)} Inline Action Edits", type="primary", use_container_width=True):
                    for uid_val, act_val in changes:
                        db.set_manual_override(uid_val, act_val, note="Inline grid edit")
                    st.toast(f"Saved {len(changes)} edits to database!", icon="💾")
                    time.sleep(0.3)
                    st.rerun()

    # -------------------------------------------------------------------------
    # MODE 3: VISUAL FOCUS CARDS
    # -------------------------------------------------------------------------
    else:
        st.markdown("### 🎴 Visual Focus Cards")
        st.caption("Inspect individual email snippets and compare AI Classifier vs Safety Auditor reasoning.")

        # Card Filter Toolbar
        f_col1, f_col2, f_col3, f_col4 = st.columns([3, 2, 2, 1])
        with f_col1:
            c_search = st.text_input("🔎 Search Cards", placeholder="Sender, subject, UID, run_id...", key="card_search_input")
        with f_col2:
            c_action = st.selectbox("Action", ["ALL", "REVIEW", "DELETE", "KEEP"], key="card_action_filter")
        with f_col3:
            c_cat = st.selectbox("Category", ["ALL", "FINANCIAL", "INVOICE", "TRAVEL", "SECURITY_OTP", "JOB_ALERT", "FOOD_TRANSIT", "MARKETING", "PERSONAL", "OTHER"], key="card_cat_filter")
        with f_col4:
            c_page_size = st.selectbox("Per Page", [25, 50, 100], index=0, key="card_page_size")

        if "card_page_num" not in st.session_state:
            st.session_state.card_page_num = 1

        c_offset = (st.session_state.card_page_num - 1) * c_page_size
        card_rows, card_total = db.get_emails_page(
            search=c_search,
            action_filter=c_action,
            category_filter=c_cat,
            limit=c_page_size,
            offset=c_offset
        )

        c_total_pages = max(1, (card_total + c_page_size - 1) // c_page_size)

        # Pagination controls
        pg_c1, pg_c2, pg_c3 = st.columns([2, 4, 2])
        with pg_c1:
            if st.button("⬅️ Previous", disabled=(st.session_state.card_page_num <= 1), key="btn_card_prev"):
                st.session_state.card_page_num -= 1
                st.rerun()
        with pg_c2:
            st.markdown(
                f"<div style='text-align: center; padding-top: 6px; font-weight: 600;'>"
                f"Page {st.session_state.card_page_num} of {c_total_pages} &nbsp;|&nbsp; {card_total:,} emails"
                f"</div>",
                unsafe_allow_html=True
            )
        with pg_c3:
            if st.button("Next ➡️", disabled=(st.session_state.card_page_num >= c_total_pages), key="btn_card_next"):
                st.session_state.card_page_num += 1
                st.rerun()

        st.markdown("---")

        if not card_rows:
            st.info("No emails match your card filter.")
        else:
            for r in card_rows:
                uid = r["uid"]
                act = (r.get("final_action") or "UNKNOWN").upper()
                if act == "DELETE":
                    badge_cls = "badge-delete"
                    ribbon_cls = "ribbon-delete"
                    act_label = "🔴 DELETE"
                elif act == "KEEP":
                    badge_cls = "badge-keep"
                    ribbon_cls = "ribbon-keep"
                    act_label = "🟢 KEEP"
                elif act == "REVIEW":
                    badge_cls = "badge-review"
                    ribbon_cls = "ribbon-review"
                    act_label = "🟡 REVIEW"
                else:
                    badge_cls = "badge-keep"
                    ribbon_cls = "ribbon-protected"
                    act_label = act

                sender_str = r.get("sender") or "Unknown"
                subj_str = r.get("subject") or "(No Subject)"
                date_str = r.get("date") or ""
                cat_str = r.get("ai_category") or "OTHER"
                conf_str = r.get("ai_confidence") or "MEDIUM"

                with st.container():
                    st.markdown(f"""
                    <div class="studio-card {ribbon_cls}">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                            <div style="flex: 1; padding-right: 12px;">
                                <div style="font-size: 0.85rem; opacity: 0.8; margin-bottom: 4px;">
                                    <b>#{uid}</b> &nbsp;•&nbsp; <b>{sender_str}</b> &nbsp;•&nbsp; <code>{date_str}</code>
                                </div>
                                <div style="font-size: 1.02rem; font-weight: 700; margin-bottom: 6px;">
                                    {subj_str}
                                </div>
                                <div>
                                    <span class="badge-category">{cat_str}</span>&nbsp;
                                    <span class="badge-confidence">{conf_str}</span>
                                </div>
                            </div>
                            <div style="text-align: right;">
                                <span class="{badge_cls}">{act_label}</span>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    btn_c1, btn_c2, btn_c3 = st.columns([6, 2, 2])
                    with btn_c1:
                        with st.expander("🔍 Rationale & Snippet Preview", expanded=False):
                            r_col1, r_col2 = st.columns(2)
                            with r_col1:
                                st.markdown(f"**Stage 2 Triage:** `{r.get('ai_decision')}` ({r.get('ai_confidence')})")
                                st.markdown(f"**AI Reason:** {r.get('ai_reason') or 'None'}")
                            with r_col2:
                                st.markdown(f"**Stage 3 Auditor:** `{r.get('validator_decision')}`")
                                st.markdown(f"**Auditor Reason:** {r.get('validator_reason') or 'None'}")
                            st.caption(f"Run ID: `{r.get('last_run_id') or 'N/A'}`")
                            st.code(r.get("snippet") or "(No snippet available)", language=None)
                    with btn_c2:
                        if st.button("🛡️ Keep", key=f"card_keep_{uid}", use_container_width=True):
                            db.set_manual_override(uid, "KEEP", note="Reviewed in card view: keep")
                            st.toast(f"Marked #{uid} as KEEP!", icon="🛡️")
                            time.sleep(0.2)
                            st.rerun()
                    with btn_c3:
                        if st.button("🗑️ Delete", key=f"card_del_{uid}", use_container_width=True):
                            db.set_manual_override(uid, "DELETE", note="Reviewed in card view: delete")
                            st.toast(f"Marked #{uid} as DELETE!", icon="🗑️")
                            time.sleep(0.2)
                            st.rerun()

                st.markdown("<div style='margin-bottom: 8px;'></div>", unsafe_allow_html=True)


# =============================================================================
# TAB 3: TRASH VAULT & RESTORE CENTER
# =============================================================================
with tab_vault:
    st.markdown("## 🛡️ Trash Vault & Restore Center")
    st.caption("Granular status-targeted deletion, safe simulation, and 1-click Message-ID restoration.")

    del_breakdown = db.get_deletions_breakdown()
    del_options = ["CONFIDENT_DELETE", "PROBABLE_DELETE", "NEEDS_REVIEW", "MANUAL_DELETE"]
    status_labels = {
        "CONFIDENT_DELETE": f"🔴 Confident Delete ({del_breakdown.get('confident_delete', 0):,} emails)",
        "PROBABLE_DELETE": f"🟠 Probable Delete ({del_breakdown.get('probable_delete', 0):,} emails)",
        "NEEDS_REVIEW": f"🟡 Needs Review ({del_breakdown.get('needs_review', 0):,} emails)",
        "MANUAL_DELETE": f"🔵 Manual Overrides ({del_breakdown.get('manual_delete', 0):,} emails)",
    }

    vault_col1, vault_col2 = st.columns(2)

    with vault_col1:
        st.markdown("### 1. Select Statuses to Trash")
        st.caption("Choose which categories of emails will be moved to Gmail Trash.")

        selected_statuses = st.multiselect(
            "Statuses to include for deletion:",
            options=del_options,
            default=["CONFIDENT_DELETE", "PROBABLE_DELETE", "MANUAL_DELETE"] if (del_breakdown.get('confident_delete', 0) > 0 or del_breakdown.get('probable_delete', 0) > 0) else ["CONFIDENT_DELETE"],
            format_func=lambda s: status_labels.get(s, s),
            key="vault_target_del_statuses",
            help="Select which status categories will be moved to Gmail Trash. Ambiguous 'Needs Review' is excluded by default for safety."
        )

        matching_to_delete = db.get_confirmed_deletions(statuses=selected_statuses) if selected_statuses else []
        target_count = len(matching_to_delete)

        if not selected_statuses:
            st.warning("⚠️ No status selected. Please check at least one status to proceed.")
        elif target_count == 0:
            st.info("ℹ️ 0 emails match the selected criteria in the database.")
        else:
            st.success(f"🎯 **{target_count:,} emails** targeted for deletion across {len(selected_statuses)} selected categories.")

        # Dry-run Preview
        st.markdown("<hr style='margin: 12px 0;'/>", unsafe_allow_html=True)
        st.markdown("### 2. Simulation (Dry-Run)")
        st.caption("Test the deletion batch without touching your Gmail inbox.")
        if st.button(f"🔍 Run Dry-Run Preview ({target_count:,} emails)", disabled=(status["is_running"] or target_count == 0), use_container_width=True, key="btn_vault_dry_run"):
            def _task_dry_run(run_id=None):
                return run_delete(input_file=None, dry_run=True, email_addr=target_account, run_id=run_id, statuses=selected_statuses)

            started = worker.start_task(
                f"Dry-Run Preview ({target_count:,} emails)",
                _task_dry_run,
                account=target_account,
                run_type="Dry-Run Preview",
                run_params={"statuses": selected_statuses, "target_count": target_count}
            )
            if started:
                st.toast("Dry Run launched in background!", icon="🔍")
                st.rerun()

    with vault_col2:
        st.markdown("### 3. Move to Gmail Trash")
        st.caption("Emails are labeled with `\\Trash` in Gmail. Google retains them for 30 days before permanent deletion.")

        confirm_trash = st.checkbox(
            f"⚠️ I confirm trashing {target_count:,} emails in Gmail",
            value=False,
            disabled=(status["is_running"] or target_count == 0),
            key="chk_vault_confirm"
        )

        if st.button(
            f"🗑️ Move {target_count:,} Emails to Gmail Trash",
            disabled=(status["is_running"] or not confirm_trash or target_count == 0),
            type="primary",
            use_container_width=True,
            key="btn_vault_exec_trash"
        ):
            def _task_trash(run_id=None):
                return run_delete(input_file=None, dry_run=False, email_addr=target_account, run_id=run_id, statuses=selected_statuses)

            started = worker.start_task(
                f"Move to Gmail Trash ({target_count:,} emails)",
                _task_trash,
                account=target_account,
                run_type="Gmail Trash Execution",
                run_params={"statuses": selected_statuses, "target_count": target_count}
            )
            if started:
                st.toast("Live deletion started in background!", icon="🗑️")
                st.rerun()

        st.markdown("<hr style='margin: 12px 0;'/>", unsafe_allow_html=True)

        st.markdown("### 4. Deterministic Undo & Restoration")
        st.caption("Restores previously trashed emails by matching permanent RFC 822 `Message-ID`s.")
        if st.button("↩️ Restore Trashed Emails to Inbox", disabled=status["is_running"], use_container_width=True, key="btn_vault_restore"):
            def _task_restore(run_id=None):
                inp = get_latest_artifact("5_processed", target_account)
                return run_restore(input_file=inp, dry_run=False, email_addr=target_account, run_id=run_id)

            started = worker.start_task("Restore Emails to Inbox", _task_restore, account=target_account, run_type="Restore Execution")
            if started:
                st.toast("Restoration started in background!", icon="↩️")
                st.rerun()


# =============================================================================
# TAB 4: ACCOUNTS & MAINTENANCE
# =============================================================================
with tab_settings:
    st.markdown("## ⚙️ Accounts & Maintenance")
    st.caption("Manage onboarded inboxes, credentials, database hygiene, and export tools.")

    st.markdown("### 👥 Configured Gmail Accounts")
    acc_list = db.list_accounts()
    if acc_list:
        acc_data = []
        for a in acc_list:
            acc_data.append({
                "Email": a["email"],
                "Display Name": a.get("display_name", ""),
                "Default": "★ Default" if a.get("is_default") else "",
                "Last UID": a.get("last_uid_scanned", 0),
                "Total Scanned": a.get("total_scanned", 0),
                "Last Fetched": a.get("last_fetched_at", "-"),
            })
        st.dataframe(pd.DataFrame(acc_data), use_container_width=True, hide_index=True)

        acc_act1, acc_act2 = st.columns(2)
        with acc_act1:
            non_defaults = [a["email"] for a in acc_list if not a.get("is_default")]
            if non_defaults:
                target_def = st.selectbox("Set as Default Account", options=non_defaults, key="set_def_acc_select")
                if st.button("⭐ Make Default Account", key="btn_make_default"):
                    db.set_default_account(target_def)
                    st.toast(f"'{target_def}' is now default!", icon="⭐")
                    time.sleep(0.4)
                    st.rerun()
        with acc_act2:
            removables = [a["email"] for a in acc_list if len(acc_list) > 1]
            if removables:
                target_rem = st.selectbox("Remove Account", options=removables, key="rem_acc_select")
                if st.button("🗑️ Remove Account Credentials", key="btn_remove_acc"):
                    db.delete_account(target_rem)
                    st.toast(f"Removed account '{target_rem}'", icon="🗑️")
                    time.sleep(0.4)
                    st.rerun()

    st.markdown("---")

    # CSV Import/Export Tools
    st.markdown("### 💾 CSV Data Utilities")
    csv_col1, csv_col2 = st.columns(2)
    with csv_col1:
        st.markdown("**📥 Import CSV Review Artifact**")
        st.caption("Syncs external review CSVs into SQLite.")
        default_csv = get_latest_artifact("4_revalidate", target_account) or ""
        csv_in_path = st.text_input("CSV File Path", value=default_csv, key="csv_import_path_input")
        if st.button("📥 Import into SQLite", key="btn_import_csv_util"):
            if os.path.isfile(csv_in_path):
                with st.spinner("Importing..."):
                    cnt = db.import_from_csv(csv_in_path)
                    st.success(f"Imported {cnt:,} emails into database!")
                    time.sleep(0.4)
                    st.rerun()
            else:
                st.error("File does not exist.")

    with csv_col2:
        st.markdown("**📤 Export Database to CSV**")
        st.caption("Extracts database records into a clean CSV file.")
        exp_act = st.selectbox("Filter Action", ["ALL", "KEEP", "DELETE"], key="csv_exp_act_select")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_a = target_account.replace("@", "_").replace(".", "_")
        exp_path = st.text_input("Export Path", value=f"db_export_{safe_a}_{ts}.csv", key="csv_exp_path_input")
        if st.button("📤 Export to CSV", key="btn_export_csv_util"):
            with st.spinner("Exporting..."):
                cnt = db.export_to_csv(exp_path, final_action=None if exp_act == "ALL" else exp_act)
                st.success(f"Exported {cnt:,} emails to `{exp_path}`!")

    st.markdown("---")

    # Reset Database
    st.markdown("### ⚠️ Clean & Reset Database (Fresh Start)")
    st.warning("Permanently wipes all local email records and resets the fetch cursor back to 0.")
    rst_c1, rst_c2 = st.columns([3, 1])
    with rst_c1:
        confirm_rst = st.checkbox("I confirm that I want to wipe all local records and start fresh", value=False, key="chk_wipe_db")
    with rst_c2:
        if st.button("🗑️ Reset Database", disabled=(not confirm_rst or status["is_running"]), type="primary", use_container_width=True, key="btn_exec_wipe"):
            with st.spinner("Resetting database..."):
                db.reset_database(reset_cursor=True)
                st.toast("Database cleaned and reset!", icon="✨")
                time.sleep(0.4)
                st.rerun()
