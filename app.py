import os
from pathlib import Path
import re
from datetime import datetime
import sqlite3
import pandas as pd
import streamlit as st


# =========================================================
# PATHS
# =========================================================
EXCEPTIONS_FILE = "/Users/yashakaushal/Documents/ASPM/Parish RAW Data/wave3/matching/matched_output/all_parishes_flat_for_ncoa_appended_match_exceptions.xlsx"
OUTPUT_DIR = "/Users/yashakaushal/Documents/ASPM/Parish RAW Data/wave3/matching/app_matching/"

DB_FILE = os.path.join(OUTPUT_DIR, "decisions.db")
DECISIONS_FILE = os.path.join(OUTPUT_DIR, "review_decisions.csv")
BINARY_FILE = os.path.join(OUTPUT_DIR, "binary_resolution.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="ASPM: Parish-RE Database Match Exceptions Review",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# =========================================================
# STYLES
# =========================================================
st.markdown("""
<style>
.block-container {
    padding-top: 1.0rem;
    padding-bottom: 2rem;
    max-width: 96%;
}
.review-toolbar {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    padding: 12px 16px;
    border-radius: 14px;
    margin-bottom: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.pill {
    display: inline-block;
    border-radius: 999px;
    padding: 4px 10px;
    margin: 2px 6px 2px 0;
    font-size: 0.80rem;
    font-weight: 700;
    border: 1px solid transparent;
}
.pill-yes {
    background: #dcfce7;
    color: #166534;
    border-color: #86efac;
}
.pill-no {
    background: #fee2e2;
    color: #991b1b;
    border-color: #fca5a5;
}
.pill-neutral {
    background: #e2e8f0;
    color: #334155;
    border-color: #cbd5e1;
}
.score-box {
    display: inline-block;
    background: #111827;
    color: white;
    border-radius: 12px;
    padding: 6px 12px;
    font-weight: 800;
    font-size: 0.95rem;
    margin-bottom: 8px;
}
</style>
""", unsafe_allow_html=True)


# =========================================================
# SQLITE SETUP
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            uniqueid TEXT PRIMARY KEY,
            decision TEXT,
            decision_binary TEXT,
            chosen_candidate_rank TEXT,
            chosen_candidate_consid TEXT,
            chosen_candidate_conscode TEXT,
            review_notes TEXT,
            reviewed_by TEXT,
            reviewed_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def export_decisions_csv():
    conn = get_conn()
    try:
        df = pd.read_sql_query("""
            SELECT
                uniqueid,
                decision,
                decision_binary,
                chosen_candidate_rank,
                chosen_candidate_consid,
                chosen_candidate_conscode,
                review_notes,
                reviewed_by,
                reviewed_at
            FROM decisions
            ORDER BY reviewed_at, uniqueid
        """, conn)
    finally:
        conn.close()

    df.to_csv(DECISIONS_FILE, index=False)


def export_binary_csv():
    conn = get_conn()
    try:
        df = pd.read_sql_query("""
            SELECT
                uniqueid,
                decision_binary,
                chosen_candidate_consid
            FROM decisions
            ORDER BY reviewed_at, uniqueid
        """, conn)
    finally:
        conn.close()

    df.to_csv(BINARY_FILE, index=False)


init_db()


# =========================================================
# HELPERS
# =========================================================
def clean_text(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    if s.lower() in {"nan", "none", "null"}:
        return ""
    return s

def normalize_name(x):
    s = clean_text(x).lower()
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def normalize_email(x):
    return clean_text(x).lower()

def normalize_phone(x):
    s = clean_text(x)
    s = re.sub(r"\D", "", s)
    return s[-10:] if len(s) >= 10 else s

def normalize_zip(x):
    s = clean_text(x)
    if not s:
        return ""
    m = re.search(r"\d{5}", s)
    return m.group(0) if m else ""

def normalize_address(x):
    s = clean_text(x).lower()
    replacements = {
        r"\bapt\b": "apartment",
        r"\bste\b": "suite",
        r"\bst\b": "street",
        r"\brd\b": "road",
        r"\bdr\b": "drive",
        r"\bln\b": "lane",
        r"\bave\b": "avenue",
        r"\bblvd\b": "boulevard",
        r"\bpkwy\b": "parkway",
        r"\bct\b": "court",
        r"\bcir\b": "circle",
        r"\btrl\b": "trail",
        r"\bpo box\b": "pobox",
        r"\bp\.o\. box\b": "pobox",
    }
    for pat, repl in replacements.items():
        s = re.sub(pat, repl, s)
    s = re.sub(r"[^a-z0-9]", "", s)
    return s

def yes_no(cond):
    return "Yes" if cond else "No"

def safe_get(row, col):
    return clean_text(row.get(col, ""))

def format_phone(x):
    s = normalize_phone(x)
    if len(s) == 10:
        return f"({s[:3]}) {s[3:6]}-{s[6:]}"
    return clean_text(x)

def extract_candidate_first_name(cand_row):
    return (
        safe_get(cand_row, "candidate_preferred_name")
        or safe_get(cand_row, "candidate_first_name")
        or ""
    )

def extract_candidate_spouse_first_name(cand_row):
    return (
        safe_get(cand_row, "candidate_sp_preferred_name")
        or safe_get(cand_row, "candidate_sp_first")
        or ""
    )


@st.cache_data
def load_exceptions(path):
    ext = Path(path).suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
    else:
        df = pd.read_excel(path, dtype=str).fillna("")
    return df


def load_existing_decisions():
    conn = get_conn()
    try:
        df = pd.read_sql_query("""
            SELECT
                uniqueid,
                decision,
                decision_binary,
                chosen_candidate_rank,
                chosen_candidate_consid,
                chosen_candidate_conscode,
                review_notes,
                reviewed_by,
                reviewed_at
            FROM decisions
        """, conn)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=[
            "uniqueid",
            "decision",
            "decision_binary",
            "chosen_candidate_rank",
            "chosen_candidate_consid",
            "chosen_candidate_conscode",
            "review_notes",
            "reviewed_by",
            "reviewed_at"
        ])

    return df.fillna("")


def save_decision(
    uniqueid,
    decision,
    chosen_candidate_rank="",
    chosen_candidate_consid="",
    chosen_candidate_conscode="",
    review_notes="",
    reviewed_by=""
):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO decisions (
                uniqueid,
                decision,
                decision_binary,
                chosen_candidate_rank,
                chosen_candidate_consid,
                chosen_candidate_conscode,
                review_notes,
                reviewed_by,
                reviewed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uniqueid) DO UPDATE SET
                decision = excluded.decision,
                decision_binary = excluded.decision_binary,
                chosen_candidate_rank = excluded.chosen_candidate_rank,
                chosen_candidate_consid = excluded.chosen_candidate_consid,
                chosen_candidate_conscode = excluded.chosen_candidate_conscode,
                review_notes = excluded.review_notes,
                reviewed_by = excluded.reviewed_by,
                reviewed_at = excluded.reviewed_at
        """, (
            str(uniqueid),
            decision,
            "1" if decision == "MATCH" else "0",
            str(chosen_candidate_rank),
            str(chosen_candidate_consid),
            str(chosen_candidate_conscode),
            str(review_notes),
            str(reviewed_by),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
        conn.commit()
    finally:
        conn.close()

    export_decisions_csv()
    export_binary_csv()


def build_candidate_score(parish_row, cand_row):
    score = 0

    parish_first = normalize_name(parish_row.get("first_name", ""))
    parish_last = normalize_name(parish_row.get("last_name", ""))
    parish_sp_first = normalize_name(parish_row.get("spouse_first", ""))
    parish_sp_last = normalize_name(parish_row.get("spouse_last", ""))

    cand_addressee = clean_text(cand_row.get("candidate_addressee", ""))
    cand_last = normalize_name(cand_row.get("candidate_last_name", ""))
    cand_sp_last = normalize_name(cand_row.get("candidate_sp_last", ""))

    parish_email = normalize_email(parish_row.get("email1", ""))
    cand_emails = {
        normalize_email(cand_row.get("candidate_email", "")),
        normalize_email(cand_row.get("candidate_email2", "")),
    }
    cand_emails.discard("")

    parish_phone = normalize_phone(parish_row.get("phone1", ""))
    cand_phones = {
        normalize_phone(cand_row.get("candidate_preferred_phone", "")),
        normalize_phone(cand_row.get("candidate_phone2", "")),
    }
    cand_phones.discard("")

    parish_addr = normalize_address(
        f"{clean_text(parish_row.get('address1', ''))} {clean_text(parish_row.get('address2', ''))}"
    )
    cand_addr = normalize_address(
        cand_row.get("candidate_address_full_ncoa_clean", "") or cand_row.get("candidate_address", "")
    )

    parish_zip = normalize_zip(parish_row.get("zip1_ncoa", "") or parish_row.get("zip1", ""))
    cand_zip = normalize_zip(cand_row.get("candidate_zip1_ncoa", "") or cand_row.get("candidate_zip", ""))

    parish_no = clean_text(parish_row.get("parish_no", ""))
    cand_conscode = clean_text(cand_row.get("candidate_conscode", ""))
    m = re.match(r"^\s*(\d+)-", cand_conscode)
    cand_parish_no = m.group(1) if m else ""

    if parish_last and (parish_last == cand_last or parish_last == cand_sp_last):
        score += 25
    if parish_sp_last and (parish_sp_last == cand_last or parish_sp_last == cand_sp_last):
        score += 10
    if parish_first and parish_first in normalize_name(cand_addressee):
        score += 20
    if parish_sp_first and parish_sp_first in normalize_name(cand_addressee):
        score += 10
    if parish_email and parish_email in cand_emails:
        score += 25
    if parish_phone and parish_phone in cand_phones:
        score += 25
    if parish_addr and cand_addr and parish_addr == cand_addr:
        score += 20
    if parish_zip and cand_zip and parish_zip == cand_zip:
        score += 10
    if parish_no and cand_parish_no and parish_no == cand_parish_no:
        score += 15

    return score


def candidate_compare_flags(parish_row, cand_row):
    parish_first = normalize_name(parish_row.get("first_name", ""))
    parish_sp_first = normalize_name(parish_row.get("spouse_first", ""))
    parish_email = normalize_email(parish_row.get("email1", ""))
    parish_phone = normalize_phone(parish_row.get("phone1", ""))
    parish_addr = normalize_address(
        f"{clean_text(parish_row.get('address1', ''))} {clean_text(parish_row.get('address2', ''))}"
    )

    cand_addressee = normalize_name(cand_row.get("candidate_addressee", ""))
    cand_emails = {
        normalize_email(cand_row.get("candidate_email", "")),
        normalize_email(cand_row.get("candidate_email2", "")),
    }
    cand_phones = {
        normalize_phone(cand_row.get("candidate_preferred_phone", "")),
        normalize_phone(cand_row.get("candidate_phone2", "")),
    }
    cand_addr = normalize_address(
        cand_row.get("candidate_address_full_ncoa_clean", "") or cand_row.get("candidate_address", "")
    )

    return {
        "First": yes_no(parish_first in cand_addressee if parish_first else False),
        "Spouse": yes_no(parish_sp_first in normalize_name(
            f"{safe_get(cand_row, 'candidate_sp_first')} {safe_get(cand_row, 'candidate_sp_preferred_name')} {safe_get(cand_row, 'candidate_addressee')}"
        ) if parish_sp_first else False),
        "Email": yes_no(parish_email in cand_emails if parish_email else False),
        "Phone": yes_no(parish_phone in cand_phones if parish_phone else False),
        "Address": yes_no(parish_addr == cand_addr if parish_addr and cand_addr else False),
        "Parish": clean_text(cand_row.get("candidate_matches_parish_no", "")) or "Unknown",
    }


def badge(label, value):
    value_l = str(value).lower()
    cls = "pill-neutral"
    if value_l == "yes":
        cls = "pill-yes"
    elif value_l == "no":
        cls = "pill-no"
    return f'<span class="pill {cls}">{label}: {value}</span>'


def render_badges(flags):
    html = "".join([badge(k, v) for k, v in flags.items()])
    st.markdown(html, unsafe_allow_html=True)


def info_rows(pairs, height=320):
    df = pd.DataFrame(pairs, columns=["Field", "Value"])
    st.dataframe(df, use_container_width=True, hide_index=True, height=height)


# =========================================================
# RENDERERS
# =========================================================
def render_parish_card(row):
    parish_name = safe_get(row, "parish_name")
    address = " ".join([x for x in [safe_get(row, "address1"), safe_get(row, "address2")] if x]).strip()

    with st.container(border=True):
        st.markdown("### Parish Record")
        st.caption("Wave 3")
        st.caption(f"{parish_name} | UniqueID: {safe_get(row, 'uniqueid')}")
        st.caption(f"{parish_name} | Parish No: {safe_get(row, 'parish_no')}")
        st.caption("")
        st.caption("")
        st.caption("")

        info_rows([
            ("First name", safe_get(row, "first_name")),
            ("Last name", safe_get(row, "last_name")),
            ("Spouse first", safe_get(row, "spouse_first")),
            ("Spouse last", safe_get(row, "spouse_last")),
            ("Mailing name", safe_get(row, "mailing_name")),
            ("Formal salutation", safe_get(row, "formal_salutation")),
            ("Address", address),
            ("City / State / ZIP", f"{safe_get(row, 'city1')}, {safe_get(row, 'state1')} {safe_get(row, 'zip1')}"),
            ("Phone", format_phone(safe_get(row, "phone1"))),
            ("Email", safe_get(row, "email1")),
            ("Parish no", safe_get(row, "parish_no")),
            ("Reason", safe_get(row, "exception_reason")),
        ], height=480)


def render_candidate_card(parish_row, cand_row):
    score = build_candidate_score(parish_row, cand_row)
    flags = candidate_compare_flags(parish_row, cand_row)

    candidate_rank = safe_get(cand_row, "candidate_rank")
    first_name = extract_candidate_first_name(cand_row)
    spouse_first = extract_candidate_spouse_first_name(cand_row)
    last_name = safe_get(cand_row, "candidate_last_name")
    spouse_last = safe_get(cand_row, "candidate_sp_last")

    address = safe_get(cand_row, "candidate_address") or safe_get(cand_row, "candidate_address_full_ncoa_clean")
    city = safe_get(cand_row, "candidate_city") or safe_get(cand_row, "candidate_city1_ncoa")
    state = safe_get(cand_row, "candidate_state") or safe_get(cand_row, "candidate_state1_ncoa")
    zip_code = safe_get(cand_row, "candidate_zip") or safe_get(cand_row, "candidate_zip1_ncoa")

    with st.container(border=True):
        st.markdown(f"### Candidate {candidate_rank}")
        st.markdown(f'<div class="score-box">Score: {score}</div>', unsafe_allow_html=True)
        st.caption(safe_get(cand_row, "candidate_conscode"))

        render_badges(flags)

        info_rows([
            ("First", first_name),
            ("Last", last_name),
            ("Spouse first", spouse_first),
            ("Spouse last", spouse_last),
            ("Addressee", safe_get(cand_row, "candidate_addressee")),
            ("Address", address),
            ("City / State / ZIP", f"{city}, {state} {zip_code}"),
            ("Phone", format_phone(safe_get(cand_row, "candidate_preferred_phone") or safe_get(cand_row, "candidate_phone2"))),
            ("Email", safe_get(cand_row, "candidate_email") or safe_get(cand_row, "candidate_email2")),
            ("ConsID", safe_get(cand_row, "candidate_consid")),
            ("ConsCode", safe_get(cand_row, "candidate_conscode")),
            ("2023 / 2024 / 2025", f"{safe_get(cand_row, 'candidate_2023_giving')} / {safe_get(cand_row, 'candidate_2024_giving')} / {safe_get(cand_row, 'candidate_2025_giving')}"),
        ], height=455)


# =========================================================
# LOAD DATA
# =========================================================
exceptions_df = load_exceptions(EXCEPTIONS_FILE)
decisions_df = load_existing_decisions()

required_cols = {"uniqueid", "candidate_rank", "candidate_consid"}
missing = required_cols - set(exceptions_df.columns)
if missing:
    st.error(f"Exceptions file missing required columns: {sorted(missing)}")
    st.stop()

exceptions_df["uniqueid"] = exceptions_df["uniqueid"].astype(str)
exceptions_df["candidate_rank"] = exceptions_df["candidate_rank"].astype(str)

resolved_ids = set(decisions_df["uniqueid"].astype(str))
all_uniqueids = list(exceptions_df["uniqueid"].dropna().astype(str).unique())
remaining_uniqueids = [u for u in all_uniqueids if u not in resolved_ids]

st.title("Parish Match Review")

st.markdown(
    f"""
    <div class="review-toolbar">
        <b>Total ambiguous:</b> {len(all_uniqueids):,}
        &nbsp;&nbsp;&nbsp; <b>Resolved:</b> {len(all_uniqueids) - len(remaining_uniqueids):,}
        &nbsp;&nbsp;&nbsp; <b>Remaining:</b> {len(remaining_uniqueids):,}
    </div>
    """,
    unsafe_allow_html=True
)

if not remaining_uniqueids:
    st.success("All records have been reviewed.")
    st.write(f"SQLite DB: `{DB_FILE}`")
    st.write(f"Decisions export: `{DECISIONS_FILE}`")
    st.write(f"Binary file: `{BINARY_FILE}`")
    st.stop()

reviewer_name = st.text_input("Reviewer name", value="", placeholder="Enter your name")

if "record_idx" not in st.session_state:
    st.session_state.record_idx = 0

max_idx = len(remaining_uniqueids) - 1

nav1, nav2, nav3 = st.columns([1, 2, 1])
with nav1:
    if st.button("Previous", disabled=st.session_state.record_idx <= 0):
        st.session_state.record_idx -= 1
with nav2:
    st.markdown(f"**Record {st.session_state.record_idx + 1} of {len(remaining_uniqueids)}**")
with nav3:
    if st.button("Next", disabled=st.session_state.record_idx >= max_idx):
        st.session_state.record_idx += 1

if remaining_uniqueids:
    st.session_state.record_idx = min(st.session_state.record_idx, len(remaining_uniqueids) - 1)
    st.session_state.record_idx = max(st.session_state.record_idx, 0)

current_uniqueid = remaining_uniqueids[st.session_state.record_idx]
grp = exceptions_df[exceptions_df["uniqueid"] == current_uniqueid].copy()

grp["_candidate_rank_num"] = pd.to_numeric(grp["candidate_rank"], errors="coerce")
grp = grp.sort_values(["_candidate_rank_num", "candidate_consid"], na_position="last")

if "candidate_matches_parish_no" in grp.columns:
    grp_filtered = grp[grp["candidate_matches_parish_no"].astype(str).str.lower() == "yes"].copy()
    if not grp_filtered.empty:
        grp = grp_filtered

parish_row = grp.iloc[0]

st.subheader("Parish vs Candidate Comparison")

candidates = list(grp.iterrows())
top_candidates = candidates[:2]
remaining_candidates = candidates[2:]

left_col, right_col = st.columns([1.05, 1.95], gap="large")

with left_col:
    render_parish_card(parish_row)

with right_col:
    top_right_cols = st.columns(2, gap="large")

    for j, (_, cand_row) in enumerate(top_candidates):
        with top_right_cols[j]:
            render_candidate_card(parish_row, cand_row)

            rank = safe_get(cand_row, "candidate_rank")
            consid = safe_get(cand_row, "candidate_consid")
            conscode = safe_get(cand_row, "candidate_conscode")

            if st.button(
                f"Match Candidate {rank}",
                key=f"match_{current_uniqueid}_{rank}_{consid}",
                use_container_width=True
            ):
                notes_val = st.session_state.get(f"notes_{current_uniqueid}", "")
                save_decision(
                    uniqueid=current_uniqueid,
                    decision="MATCH",
                    chosen_candidate_rank=rank,
                    chosen_candidate_consid=consid,
                    chosen_candidate_conscode=conscode,
                    review_notes=notes_val,
                    reviewed_by=reviewer_name,
                )
                st.success(f"Saved MATCH for candidate {rank}")
                st.rerun()

if remaining_candidates:
    blank_left, lower_right = st.columns([1.05, 1.95], gap="large")

    with blank_left:
        st.empty()

    with lower_right:
        for start in range(0, len(remaining_candidates), 2):
            row_candidates = remaining_candidates[start:start + 2]
            cand_cols = st.columns(2, gap="large")

            for j, (_, cand_row) in enumerate(row_candidates):
                with cand_cols[j]:
                    render_candidate_card(parish_row, cand_row)

                    rank = safe_get(cand_row, "candidate_rank")
                    consid = safe_get(cand_row, "candidate_consid")
                    conscode = safe_get(cand_row, "candidate_conscode")

                    if st.button(
                        f"Match Candidate {rank}",
                        key=f"match_{current_uniqueid}_{rank}_{consid}",
                        use_container_width=True
                    ):
                        notes_val = st.session_state.get(f"notes_{current_uniqueid}", "")
                        save_decision(
                            uniqueid=current_uniqueid,
                            decision="MATCH",
                            chosen_candidate_rank=rank,
                            chosen_candidate_consid=consid,
                            chosen_candidate_conscode=conscode,
                            review_notes=notes_val,
                            reviewed_by=reviewer_name,
                        )
                        st.success(f"Saved MATCH for candidate {rank}")
                        st.rerun()

st.markdown("---")
st.subheader("Decision Notes")

st.text_area(
    "Review notes",
    key=f"notes_{current_uniqueid}",
    height=120,
    placeholder="Optional comments for why you chose a candidate or marked no match..."
)

if st.button("No Match", key=f"nomatch_{current_uniqueid}", use_container_width=True):
    notes_val = st.session_state.get(f"notes_{current_uniqueid}", "")
    save_decision(
        uniqueid=current_uniqueid,
        decision="NO_MATCH",
        chosen_candidate_rank="",
        chosen_candidate_consid="",
        chosen_candidate_conscode="",
        review_notes=notes_val,
        reviewed_by=reviewer_name,
    )
    st.warning("Saved NO MATCH")
    st.rerun()

st.markdown("---")
st.subheader("Current Outputs")

d1, d2 = st.columns(2)

with d1:
    if os.path.exists(DECISIONS_FILE):
        with open(DECISIONS_FILE, "rb") as f:
            st.download_button(
                "Download review_decisions.csv",
                f,
                file_name="review_decisions.csv",
                mime="text/csv",
                use_container_width=True
            )

with d2:
    if os.path.exists(BINARY_FILE):
        with open(BINARY_FILE, "rb") as f:
            st.download_button(
                "Download binary_resolution.csv",
                f,
                file_name="binary_resolution.csv",
                mime="text/csv",
                use_container_width=True
            )