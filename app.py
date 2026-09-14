"""
Gmail AI Cleaner - Interactive Streamlit Web Dashboard.

Provides:
- Live aggregate pipeline metrics and visual breakdown.
- Fast email explorer with search, multi-field filters, and 1-click manual KEEP/DELETE overrides.
- Background task execution for all pipeline actions with live progress and real-time log streaming.
- Database and CSV import/export utilities.
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
from gmail_cleaner.logger import get_logger
from gmail_cleaner.state import get_account_dir, get_latest_artifact
from gmail_cleaner.worker import worker

# Pipeline operations
from gmail_cleaner.cli import run_all_pipeline
from gmail_cleaner.stages import run_delete, run_restore
from gmail_cleaner.streaming import run_streaming_pipeline

st.set_page_config(
    page_title="Gmail AI Cleaner",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 14px 18px;
        border-left: 5px solid #4CAF50;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .badge-keep {
        background-color: #e8f5e9;
        color: #2e7d32;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-delete {
        background-color: #ffebee;
        color: #c62828;
        padding: 3px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .stButton>button {
        border-radius: 6px;
    }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# SIDEBAR: Account & Global Controls
# -----------------------------------------------------------------------------
with st.sidebar:
    st.title("📧 Gmail AI Cleaner")
    target_account = st.text_input("Target Gmail Account", value=GMAIL_USER)
    
    db = EmailDB(account=target_account)
    db_file = db.db_path
    db_exists = os.path.isfile(db_file)

    st.markdown("---")
    st.subheader("⚡ Quick Status")
    status = worker.get_status()
    if status["is_running"]:
        st.info(f"⚙️ Running: **{status['task_name']}**")
        st.progress(status["progress_pct"] / 100.0)
        st.caption(f"{status['status_message']} ({status['elapsed_seconds']}s)")
        if st.button("🛑 Cancel Task", key="sidebar_cancel_btn", type="secondary", use_container_width=True):
            worker.request_cancel()
            st.toast("Cancellation requested!", icon="⚠️")
    else:
        # Persistent status from SQLite: survives browser reloads & resets
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

            st.caption(f"**Run ID:** `{latest_run['run_id']}`")
            st.caption(f"📅 **Time:** {latest_run.get('started_at', '')}")
            dur = latest_run.get("duration_seconds")
            if dur is not None and dur > 0:
                st.caption(f"⏱️ **Duration:** {dur:.1f}s")
            tot = latest_run.get("total_emails")
            if tot is not None and tot > 0:
                st.caption(f"📧 **Emails:** {tot:,} ({latest_run.get('delete_count', 0):,} del / {latest_run.get('keep_count', 0):,} keep)")
        else:
            st.success("🟢 Worker Idle")
            st.caption("No previous runs recorded.")

    st.markdown("---")
    st.caption(f"**SQLite Database:** `{os.path.basename(db_file)}`")
    st.caption(f"**Size:** {round(os.path.getsize(db_file) / (1024 * 1024), 2) if db_exists else 0} MB")


# -----------------------------------------------------------------------------
# TABS
# -----------------------------------------------------------------------------
tab_overview, tab_explorer, tab_runner, tab_history, tab_db_tools = st.tabs([
    "📊 Overview & Metrics",
    "🔍 Email Explorer & Overrides",
    "🚀 Pipeline Operations",
    "📜 Run History & Audit",
    "💾 Data & CSV Tools",
])


# =============================================================================
# TAB 1: OVERVIEW & METRICS
# =============================================================================
with tab_overview:
    st.header("📊 Pipeline Overview")
    
    stats = db.get_stats()
    total_emails = stats.get("total_emails", 0)

    if total_emails == 0:
        st.warning("⚠️ No emails found in the database. Go to **'Data & CSV Tools'** to import an existing review CSV or **'Pipeline Operations'** to run a fetch.")
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Emails Tracked", f"{total_emails:,}")
        with col2:
            del_count = stats.get("pending_delete", 0)
            del_pct = round((del_count / total_emails) * 100, 1) if total_emails > 0 else 0
            st.metric("Pending Deletion", f"{del_count:,}", f"{del_pct}% of total", delta_color="inverse")
        with col3:
            keep_count = stats.get("kept", 0)
            keep_pct = round((keep_count / total_emails) * 100, 1) if total_emails > 0 else 0
            st.metric("Safe to Keep", f"{keep_count:,}", f"{keep_pct}% of total")
        with col4:
            trashed_count = stats.get("trashed", 0)
            st.metric("Moved to Trash", f"{trashed_count:,}")

        st.markdown("---")

        sub_col1, sub_col2, sub_col3 = st.columns(3)
        with sub_col1:
            st.markdown("### 🛡️ Safety Protections")
            st.write(f"• **Starred Emails Protected:** `{stats.get('starred', 0):,}`")
            st.write(f"• **Thread Replies Protected:** `{stats.get('replies', 0):,}`")
        with sub_col2:
            st.markdown("### 🔄 Lifecycle Statuses")
            st.write(f"• **Fetched (Raw):** `{stats.get('fetched', 0):,}`")
            st.write(f"• **Scanned (Stage 2):** `{stats.get('scanned', 0):,}`")
            st.write(f"• **Audited (Stage 3):** `{stats.get('audited', 0):,}`")
        with sub_col3:
            st.markdown("### 💡 Recommended Next Action")
            if del_count > 0:
                st.info(f"You have **{del_count:,}** emails confirmed for deletion. Review in **'Email Explorer'** or run a **Dry Run** in **'Pipeline Operations'**.")
            else:
                st.success("All emails are classified and kept, or trash is empty!")

        # Chart Breakdown
        st.markdown("### 📈 Decision Distribution")
        chart_data = pd.DataFrame({
            "Action": ["Pending Delete", "Keep", "Already Trashed"],
            "Count": [del_count, keep_count, trashed_count]
        })
        st.bar_chart(chart_data.set_index("Action"))


# =============================================================================
# TAB 2: EMAIL EXPLORER & MANUAL OVERRIDES
# =============================================================================
with tab_explorer:
    st.header("🔍 Email Explorer & Interactive Triage")
    st.caption("Inspect emails, filter decisions, search across senders and snippets, and override AI decisions with a single click.")

    # Search and Filter Toolbar
    f_col1, f_col2, f_col3, f_col4 = st.columns([3, 2, 2, 1])
    with f_col1:
        default_search = st.session_state.get("explorer_search", "")
        search_query = st.text_input("🔎 Search (Sender, Subject, Snippet, UID, or Run ID)", value=default_search)
        if default_search and search_query != default_search:
            st.session_state["explorer_search"] = search_query
    with f_col2:
        action_filter = st.selectbox("Action Filter", ["ALL", "DELETE", "KEEP"], index=0)
    with f_col3:
        status_filter = st.selectbox("Status Filter", ["ALL", "FETCHED", "SCANNED", "AUDITED", "TRASHED", "RESTORED"], index=0)
    with f_col4:
        page_size = st.selectbox("Per Page", [25, 50, 100], index=0)

    # Pagination state
    if "page_num" not in st.session_state:
        st.session_state.page_num = 1

    # Reset page on filter change
    filter_key = f"{search_query}_{action_filter}_{status_filter}_{page_size}"
    if "last_filter_key" not in st.session_state or st.session_state.last_filter_key != filter_key:
        st.session_state.last_filter_key = filter_key
        st.session_state.page_num = 1

    offset = (st.session_state.page_num - 1) * page_size
    rows, total_matches = db.get_emails_page(
        search=search_query,
        action_filter=action_filter,
        status_filter=status_filter,
        limit=page_size,
        offset=offset,
    )

    total_pages = max(1, (total_matches + page_size - 1) // page_size)

    # Pagination header controls
    p_col1, p_col2, p_col3 = st.columns([2, 4, 2])
    with p_col1:
        if st.button("⬅️ Previous Page", disabled=(st.session_state.page_num <= 1)):
            st.session_state.page_num -= 1
            st.rerun()
    with p_col2:
        st.markdown(
            f"<div style='text-align: center; padding-top: 6px;'>"
            f"Page <b>{st.session_state.page_num}</b> of <b>{total_pages}</b> &nbsp;|&nbsp; <b>{total_matches:,}</b> matching emails"
            f"</div>",
            unsafe_allow_html=True
        )
    with p_col3:
        if st.button("Next Page ➡️", disabled=(st.session_state.page_num >= total_pages)):
            st.session_state.page_num += 1
            st.rerun()

    st.markdown("---")

    if not rows:
        st.info("No emails match your search and filter criteria.")
    else:
        for r in rows:
            uid = r["uid"]
            action = (r.get("final_action") or "UNKNOWN").upper()
            badge_class = "badge-delete" if action == "DELETE" else "badge-keep"
            sender = r.get("sender") or "Unknown"
            subject = r.get("subject") or "(No Subject)"
            date_str = r.get("date") or ""

            # Card Header
            with st.container():
                c1, c2, c3 = st.columns([6, 2, 2])
                with c1:
                    st.markdown(f"**#{uid}** &nbsp;•&nbsp; **{sender}** &nbsp;•&nbsp; `{date_str}`")
                    st.write(f"**{subject}**")
                with c2:
                    st.markdown(f"<span class='{badge_class}'>{action}</span>", unsafe_allow_html=True)
                    st.caption(f"Status: {r.get('status')}")
                with c3:
                    # Toggle Action Buttons
                    if action == "DELETE":
                        if st.button("🛡️ Keep Email", key=f"btn_keep_{uid}", use_container_width=True):
                            db.set_manual_override(uid, "KEEP", note="User manual keep via UI")
                            st.toast(f"Marked UID {uid} as KEEP!", icon="✅")
                            time.sleep(0.3)
                            st.rerun()
                    else:
                        if st.button("🗑️ Delete Email", key=f"btn_del_{uid}", use_container_width=True):
                            db.set_manual_override(uid, "DELETE", note="User manual delete via UI")
                            st.toast(f"Marked UID {uid} as DELETE!", icon="🗑️")
                            time.sleep(0.3)
                            st.rerun()

                # Expandable details
                with st.expander("🔍 View AI Reasoning & Snippet"):
                    d_col1, d_col2 = st.columns(2)
                    with d_col1:
                        st.markdown(f"**AI Decision:** `{r.get('ai_decision')}`")
                        st.markdown(f"**AI Reason:** {r.get('ai_reason') or 'None'}")
                    with d_col2:
                        st.markdown(f"**Auditor Decision:** `{r.get('validator_decision')}`")
                        st.markdown(f"**Auditor Reason:** {r.get('validator_reason') or 'None'}")
                    
                    if r.get("revalidation_notes"):
                        st.caption(f"Revalidation Notes: {r.get('revalidation_notes')}")
                    if r.get("last_run_id"):
                        st.caption(f"Run ID: `{r.get('last_run_id')}`")
                    
                    st.markdown("**Email Snippet:**")
                    st.code(r.get("snippet") or "(No snippet available)", language=None)
                st.markdown("<hr style='margin: 8px 0; border: none; border-top: 1px solid #e0e0e0;' />", unsafe_allow_html=True)


# =============================================================================
# TAB 3: PIPELINE OPERATIONS & RUNNER
# =============================================================================
with tab_runner:
    st.header("🚀 Pipeline Operations & Live Execution")
    st.caption("Trigger background pipeline actions without freezing the dashboard. Monitor real-time logs and progress.")

    runner_status = worker.get_status()

    # Active Task Banner
    if runner_status["is_running"]:
        st.warning(f"⏳ **Active Task:** {runner_status['task_name']} — {runner_status['status_message']}")
        st.progress(runner_status["progress_pct"] / 100.0)
        col_c1, col_c2 = st.columns([4, 1])
        with col_c1:
            st.write(f"Elapsed: **{runner_status['elapsed_seconds']}s** | Progress: **{runner_status['progress_current']} / {runner_status['progress_total']}**")
        with col_c2:
            if st.button("🛑 Cancel Task", key="btn_cancel_runner", type="primary", use_container_width=True):
                worker.request_cancel()
                st.toast("Cancellation requested!", icon="🛑")
                st.rerun()
    elif runner_status.get("error"):
        st.error(f"❌ Previous task failed: {runner_status['error']}")

    st.markdown("---")

    # Operations Grid
    op_col1, op_col2 = st.columns(2)

    with op_col1:
        st.subheader("1. Streaming Pipeline (Overlapped Fetch & AI)")
        st.caption("Fetches emails, classifies with Gemini, audits safety, and writes to SQLite & CSV in real-time.")
        
        stream_limit = st.number_input("Limit (emails)", min_value=10, max_value=50000, value=500, step=50, key="stream_limit")
        
        exec_mode = st.radio(
            "AI Processing Mode",
            [
                "🎯 Individual (1 Email / Call) — Maximum Consistency & Zero Batch Bleed",
                "⚡ Batched (Bulk Requests) — Maximum Speed"
            ],
            index=0,
            key="stream_exec_mode",
            help="Individual mode evaluates each email in its own isolated LLM prompt to guarantee repeatable, consistent decisions. Batched groups emails into fewer API calls."
        )
        
        if "Individual" in exec_mode:
            stream_batch_size = 1
            cost_est = stream_limit * 0.000375
            st.info(f"💰 **Estimated Cost:** **${cost_est:.3f}** (~₹{cost_est * 87:.1f}) for {stream_limit:,} emails\n*(Zero Thinking Tokens + Temp 0.0)*")
        else:
            stream_batch_size = st.slider("Batch Size (emails/request)", min_value=5, max_value=50, value=25, step=5, key="stream_batch_size")
            cost_est = stream_limit * 0.00018
            st.info(f"💰 **Estimated Cost:** **${cost_est:.3f}** (~₹{cost_est * 87:.1f}) for {stream_limit:,} emails\n*(Zero Thinking Tokens + Temp 0.0)*")

        col_w1, col_w2 = st.columns(2)
        with col_w1:
            stream_workers = st.slider("Gemini Workers", min_value=1, max_value=20, value=DEFAULT_MAX_WORKERS, key="stream_workers")
        with col_w2:
            stream_tier = st.selectbox("API Tier", ["paid", "free"], index=0, key="stream_tier")

        if st.button("▶️ Start Streaming Pipeline", disabled=runner_status["is_running"], key="btn_start_stream"):
            started = worker.start_task(
                f"Streaming Pipeline ({stream_limit} emails, batch={stream_batch_size})",
                run_streaming_pipeline,
                limit=stream_limit,
                batch_size=stream_batch_size,
                workers=stream_workers,
                tier=stream_tier,
                email_addr=target_account,
                account=target_account,
                run_type="Streaming Pipeline",
                run_params={"limit": stream_limit, "batch_size": stream_batch_size, "workers": stream_workers, "tier": stream_tier},
            )
            if started:
                st.toast("Streaming Pipeline launched in background!", icon="🚀")
                st.rerun()

        st.markdown("---")

        st.subheader("2. Re-Scan Kept Emails")
        st.caption("Re-evaluates emails currently marked as KEEP with updated classification prompts (0 IMAP calls).")
        if st.button("🔄 Re-Scan Kept Emails", disabled=runner_status["is_running"], key="btn_rescan_kept"):
            def _task_rescan(run_id=None):
                db_inst = EmailDB(account=target_account)
                reset_count = db_inst.reset_kept_for_rescan()
                logger = get_logger("ui")
                logger.info(f"Reset {reset_count} KEPT emails in DB. Running scan...")
                from gmail_cleaner.stages import run_scan, run_validate, run_revalidate
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                temp_csv = os.path.join(get_account_dir(target_account), "1_fetch", f"rescan_input_{timestamp}.csv")
                db_inst.export_to_csv(temp_csv, status="FETCHED")
                s_file = run_scan(input_file=temp_csv, workers=10, email_addr=target_account, only_kept=False)
                if s_file:
                    v_file = run_validate(input_file=s_file, workers=10, email_addr=target_account)
                    if v_file:
                        r_file = run_revalidate(input_file=v_file, email_addr=target_account)
                        db_inst.import_from_csv(r_file)
                return "Re-scan complete."

            started = worker.start_task(
                "Re-Scan Kept Emails",
                _task_rescan,
                account=target_account,
                run_type="Re-Scan Kept"
            )
            if started:
                st.toast("Re-Scan Kept task launched in background!", icon="🔄")
                st.rerun()

    with op_col2:
        st.subheader("3. Deletion Preview (Dry-Run)")
        st.caption("Simulates deletion of all confirmed emails directly from SQLite without touching Gmail.")
        if st.button("🔍 Run Dry-Run Preview", disabled=runner_status["is_running"], key="btn_dry_run"):
            def _task_dry_run(run_id=None):
                return run_delete(input_file=None, dry_run=True, email_addr=target_account, run_id=run_id)

            started = worker.start_task(
                "Dry-Run Simulation",
                _task_dry_run,
                account=target_account,
                run_type="Dry-Run Preview"
            )
            if started:
                st.toast("Dry Run launched in background!", icon="🔍")
                st.rerun()

        st.markdown("---")

        st.subheader("4. Move Confirmed to Gmail Trash")
        st.caption("Permanently moves confirmed deletion candidates directly from SQLite to Gmail Trash (honors all manual UI overrides).")
        confirm_del = st.checkbox("⚠️ I have reviewed the emails and confirm trashing them in Gmail", value=False)
        if st.button("🗑️ Move to Gmail Trash", disabled=(runner_status["is_running"] or not confirm_del), type="primary", key="btn_exec_trash"):
            def _task_trash(run_id=None):
                return run_delete(input_file=None, dry_run=False, email_addr=target_account, run_id=run_id)

            started = worker.start_task(
                "Move to Gmail Trash",
                _task_trash,
                account=target_account,
                run_type="Gmail Trash Execution"
            )
            if started:
                st.toast("Live deletion started in background!", icon="🗑️")
                st.rerun()

        st.markdown("---")

        st.subheader("5. Undo / Restore from Trash")
        st.caption("Restores previously deleted emails from Gmail Trash back to your Inbox.")
        if st.button("↩️ Undo Trashing", disabled=runner_status["is_running"], key="btn_restore"):
            def _task_restore(run_id=None):
                inp = get_latest_artifact("5_processed", target_account)
                return run_restore(input_file=inp, dry_run=False, email_addr=target_account)

            started = worker.start_task(
                "Restore Emails to Inbox",
                _task_restore,
                account=target_account,
                run_type="Restore Execution"
            )
            if started:
                st.toast("Restoration started in background!", icon="↩️")
                st.rerun()

    # Real-time Log Streamer
    st.markdown("---")
    st.subheader("📜 Live Operation Logs (`cleaner.log`)")
    log_lines = worker.tail_logs(target_account, lines=40)
    st.text_area("Log Output", value="".join(log_lines), height=250, disabled=True)
    if st.button("🔄 Refresh Logs", key="btn_refresh_logs"):
        st.rerun()

    # Auto-rerun loop if task is active
    if runner_status["is_running"]:
        time.sleep(1.5)
        st.rerun()


# =============================================================================
# TAB 4: RUN HISTORY & AUDIT
# =============================================================================
with tab_history:
    st.header("📜 Run History & Execution Audit")
    st.caption("Inspect past pipeline executions, compare run statistics, inspect parameters, and preview generated CSV review artifacts.")

    run_metrics = db.get_run_metrics_summary()
    total_runs = run_metrics.get("total_runs", 0)

    # Summary KPI cards
    h_col1, h_col2, h_col3, h_col4 = st.columns(4)
    with h_col1:
        st.metric("Total Executions", f"{total_runs:,}")
    with h_col2:
        st.metric("Completed Runs", f"{run_metrics.get('completed_runs', 0):,}")
    with h_col3:
        failed_cancelled = run_metrics.get("failed_runs", 0) + run_metrics.get("cancelled_runs", 0)
        st.metric("Failed / Cancelled", f"{failed_cancelled:,}", delta_color="inverse")
    with h_col4:
        st.metric("Total Emails Handled", f"{run_metrics.get('total_emails_handled', 0):,}")

    st.markdown("---")

    runs = db.get_runs(limit=100)
    if not runs:
        st.info("No runs recorded yet. Execute any pipeline operation in **'Pipeline Operations'** to begin tracking.")
    else:
        st.subheader("📋 Recent Pipeline Executions")

        # Format table for display
        table_rows = []
        for r in runs:
            st_badge = "✅ COMPLETED" if r["status"] == "COMPLETED" else ("❌ FAILED" if r["status"] == "FAILED" else ("🛑 CANCELLED" if r["status"] == "CANCELLED" else "⚙️ RUNNING"))
            table_rows.append({
                "Run ID": r["run_id"],
                "Action": r["action_type"],
                "Status": st_badge,
                "Started At": r.get("started_at") or "",
                "Duration": f"{round(r.get('duration_seconds') or 0.0, 1)}s",
                "Total Emails": r.get("total_emails") or 0,
                "Deletes": r.get("delete_count") or 0,
                "Keeps": r.get("keep_count") or 0,
                "Trashed": r.get("trashed_count") or 0,
                "Artifact": os.path.basename(r["artifact_path"]) if r.get("artifact_path") else "-",
            })

        df_runs = pd.DataFrame(table_rows)
        st.dataframe(df_runs, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("🔍 Run Inspector")

        run_id_list = [r["run_id"] for r in runs]
        selected_run_id = st.selectbox(
            "Select Run ID to Inspect",
            run_id_list,
            format_func=lambda x: f"{x}  —  {next((r['action_type'] for r in runs if r['run_id'] == x), '')}",
            key="inspector_run_select"
        )

        selected_run = db.get_run(selected_run_id)
        if selected_run:
            i_col1, i_col2, i_col3 = st.columns(3)
            with i_col1:
                st.markdown(f"**Action Type:** `{selected_run['action_type']}`")
                st.markdown(f"**Status:** `{selected_run['status']}`")
                st.markdown(f"**Run ID:** `{selected_run['run_id']}`")
            with i_col2:
                st.markdown(f"**Started At:** `{selected_run.get('started_at') or 'N/A'}`")
                st.markdown(f"**Completed At:** `{selected_run.get('completed_at') or 'N/A'}`")
                st.markdown(f"**Duration:** `{round(selected_run.get('duration_seconds') or 0.0, 1)}s`")
            with i_col3:
                st.markdown(f"**Total Processed:** `{selected_run.get('total_emails', 0):,}`")
                st.markdown(f"**Deletes / Keeps:** `{selected_run.get('delete_count', 0):,}` / `{selected_run.get('keep_count', 0):,}`")
                st.markdown(f"**Moved to Trash:** `{selected_run.get('trashed_count', 0):,}`")

            if selected_run.get("error_message"):
                st.error(f"❌ **Error Traceback:** {selected_run['error_message']}")

            if selected_run.get("params_json") and selected_run["params_json"] not in ("{}", None):
                with st.expander("⚙️ Execution Parameters"):
                    try:
                        st.json(json.loads(selected_run["params_json"]))
                    except Exception:
                        st.text(selected_run["params_json"])

            if selected_run.get("notes"):
                st.caption(f"📝 Notes: {selected_run['notes']}")

            # Filter Emails in Tab 2 button
            btn_col1, btn_col2 = st.columns([2, 5])
            with btn_col1:
                if st.button("🔎 Filter Email Explorer by this Run", key=f"btn_filter_run_{selected_run_id}"):
                    st.session_state["explorer_search"] = selected_run_id
                    st.toast(f"Explorer search set to Run ID `{selected_run_id}`. Switch to Tab 2 to view.", icon="🔍")

            # Artifact Preview
            art_path = selected_run.get("artifact_path")
            if art_path and os.path.isfile(art_path):
                st.markdown(f"#### 📄 Artifact Preview: `{os.path.basename(art_path)}`")
                st.caption(f"Full path: `{art_path}` ({round(os.path.getsize(art_path) / 1024, 1)} KB)")
                try:
                    df_preview = pd.read_csv(art_path, nrows=15)
                    st.dataframe(df_preview, use_container_width=True)
                    
                    with open(art_path, "rb") as f_art:
                        st.download_button(
                            label=f"⬇️ Download {os.path.basename(art_path)}",
                            data=f_art,
                            file_name=os.path.basename(art_path),
                            mime="text/csv",
                            key=f"dl_{selected_run_id}",
                        )
                except Exception as err:
                    st.warning(f"Could not load preview for artifact: {err}")


# =============================================================================
# TAB 5: DATABASE & CSV TOOLS
# =============================================================================
with tab_db_tools:
    st.header("💾 Database & CSV Management")
    st.caption("Import existing review CSVs into SQLite, export database subsets, or manage local storage.")

    tool_col1, tool_col2 = st.columns(2)

    with tool_col1:
        st.subheader("📥 Import CSV into SQLite DB")
        st.write("Syncs any pipeline CSV artifact (`4_revalidate`, `2_scan`, etc.) into `emails.db`.")
        
        default_csv = get_latest_artifact("4_revalidate", target_account) or ""
        csv_input_path = st.text_input("CSV File Path", value=default_csv)

        if st.button("📥 Import into Database", key="btn_import_csv"):
            if not os.path.isfile(csv_input_path):
                st.error(f"File does not exist: {csv_input_path}")
            else:
                with st.spinner("Importing into SQLite database..."):
                    imported = db.import_from_csv(csv_input_path)
                    st.success(f"Successfully imported {imported:,} emails into SQLite DB!")
                    st.rerun()

    with tool_col2:
        st.subheader("📤 Export SQLite DB to CSV")
        st.write("Extracts a clean CSV artifact from the SQLite database.")

        exp_action = st.selectbox("Export Action Filter", ["ALL", "KEEP", "DELETE"], key="exp_action")
        exp_status = st.selectbox("Export Status Filter", ["ALL", "FETCHED", "SCANNED", "AUDITED", "TRASHED", "RESTORED"], key="exp_status")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_out = os.path.join(get_account_dir(target_account), f"db_export_{timestamp}.csv")
        export_out_path = st.text_input("Output CSV Path", value=default_out)

        if st.button("📤 Export Database to CSV", key="btn_export_csv"):
            act = None if exp_action == "ALL" else exp_action
            stat = None if exp_status == "ALL" else exp_status
            with st.spinner("Exporting from SQLite database..."):
                exported = db.export_to_csv(export_out_path, final_action=act, status=stat)
                st.success(f"Successfully exported {exported:,} emails to `{export_out_path}`!")

    st.markdown("---")
    st.subheader("🧹 Database Hygiene & Maintenance")
    h_col1, h_col2 = st.columns([3, 1])
    with h_col1:
        st.write("Scan and clean any residual MIME multipart boundary delimiters (`------=_Part...`) or subheaders (`Content-Type:`) across all emails stored in SQLite.")
    with h_col2:
        if st.button("🧹 Clean All Snippets", key="btn_clean_snippets", use_container_width=True):
            with st.spinner("Cleaning snippets in SQLite DB..."):
                cleaned = db.clean_existing_snippets()
                st.success(f"Cleaned {cleaned:,} email snippets in database!")
                time.sleep(0.5)
                st.rerun()

    st.markdown("---")
    st.subheader("📥 Refill Missing Snippets from Gmail")

    with db.get_connection() as conn:
        missing_count = conn.execute(
            "SELECT COUNT(*) FROM emails WHERE account = ? AND (snippet IS NULL OR snippet = '' OR snippet = '(No snippet available)') AND status != 'TRASHED'",
            (target_account,)
        ).fetchone()[0]

    b_col1, b_col2 = st.columns([3, 1])
    with b_col1:
        st.write(f"Currently **{missing_count:,}** emails in the database have an empty snippet. Backfill downloads 10KB body slices via safe IMAP and populates clean snippets without modifying any decisions.")
        bf_limit = st.number_input("Max emails to backfill (0 for all)", min_value=0, max_value=50000, value=500, step=100, key="bf_limit")
    with b_col2:
        st.write("")
        st.write("")
        if st.button("📥 Refill Snippets", disabled=(runner_status["is_running"] or missing_count == 0), key="btn_backfill_snippets", use_container_width=True):
            def _task_backfill(run_id=None):
                db_inst = EmailDB(account=target_account)
                lim = None if bf_limit == 0 else bf_limit
                updated = db_inst.backfill_missing_snippets(batch_size=100, limit=lim)
                if run_id:
                    db_inst.update_run(run_id, total_emails=updated, status="COMPLETED")
                return updated

            started = worker.start_task(
                f"Refill Snippets ({bf_limit if bf_limit > 0 else 'All'})",
                _task_backfill,
                account=target_account,
                run_type="Snippet Backfill",
                run_params={"limit": bf_limit},
            )
            if started:
                st.toast("Snippet backfill started in background!", icon="📥")
                st.rerun()
