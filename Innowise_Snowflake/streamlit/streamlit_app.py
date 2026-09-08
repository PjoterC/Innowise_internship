"""Streamlit in Snowflake — clone a database and grant it out in one go.

A zero-copy `CREATE DATABASE ... CLONE` is one statement; making the copy
usable by anyone other than whoever ran it is a hundred more. This app collects
the four things that actually vary — source, target, the role that owns the
clone, and the roles that may read it — and hands them to
`TOOLING.APPS.SP_CLONE_DATABASE`, which builds and runs the rest.

Every value below is bound as a procedure parameter, never concatenated into
SQL here: the plan, the identifier whitelist and the grant matrix all live in
streamlit/sp_clone_database.sql, so a worksheet or an Airflow task gets the
same behaviour as this form. This file is the form and the report.

Deployed by streamlit/deploy.sql — see streamlit/README.md.
"""

import json
import re
from datetime import datetime

import streamlit as st
from snowflake.snowpark.context import get_active_session

PROCEDURE = "TOOLING.APPS.SP_CLONE_DATABASE"
PLACEHOLDERS = ", ".join(["?"] * 12)
TYPE_IT = "— choose a name —"

# Mirrors TOOLING.APPS.F_IDENTIFIER, and is the one piece of validation that is
# duplicated here rather than left to the procedure: SHOW needs an identifier,
# an identifier cannot be a bind variable, and the policy query below never
# reaches the procedure that would otherwise be doing the checking.
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,254}$")
MAX_POLICIES = 20

st.set_page_config(page_title="Database clone", page_icon="❄️", layout="centered")

session = get_active_session()


def call_procedure(params: tuple, dry_run: bool) -> dict:
    """One CALL. Returns the procedure's VARIANT result, already parsed."""
    row = session.sql(
        f"CALL {PROCEDURE}({PLACEHOLDERS})", params=[*params, dry_run]
    ).collect()[0]
    return json.loads(row[0])


@st.cache_data(show_spinner=False, ttl=60)
def preview(params: tuple) -> dict:
    """The plan, without running it.

    Cached on the parameter tuple because Streamlit re-runs this script on every
    keystroke and the plan only changes when a field does — otherwise the form
    would issue a query per character typed.
    """
    return call_procedure(params, dry_run=True)


@st.cache_data(show_spinner=False, ttl=300)
def show_names(command: str, column: str) -> list[str]:
    """Run a SHOW command and return one column of it.

    Failures return an empty list rather than raising: these lists are a
    convenience, not a requirement. A role that cannot see every warehouse in
    the account can still type the name of the one it uses.
    """
    try:
        rows = session.sql(command).collect()
    except Exception:  # noqa: BLE001 — a missing privilege is not an error here
        return []
    return sorted(str(row[column]) for row in rows)


@st.cache_data(show_spinner=False, ttl=300)
def source_policies(database: str) -> list[dict]:
    """Policies defined in the source database, and the objects they guard.

    Advisory, and deliberately read here rather than inside the plan builder: it
    describes the *source*, nothing in the plan depends on it, and the plan is
    rebuilt far more often than the source selection changes.

    SHOW lists the policies; POLICY_REFERENCES says what each one is attached
    to. Neither reads SNOWFLAKE.ACCOUNT_USAGE, so there is no replication lag to
    reason about — a policy attached a minute ago is already visible here.
    """
    name = (database or "").strip().strip('"').upper()
    if not IDENTIFIER_RE.match(name):
        return []
    found: list[dict] = []
    for kind, show in (("row access", "ROW ACCESS POLICIES"),
                       ("masking", "MASKING POLICIES")):
        try:
            rows = session.sql(f"SHOW {show} IN DATABASE {name}").collect()
        except Exception:  # noqa: BLE001 — not being allowed to look is not an error
            continue
        for row in rows[:MAX_POLICIES]:
            fqn = f'{row["database_name"]}.{row["schema_name"]}.{row["name"]}'
            try:
                refs = session.sql(
                    "SELECT REF_ENTITY_NAME FROM TABLE("
                    f"{name}.INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME => ?))",
                    params=[fqn],
                ).collect()
            except Exception:  # noqa: BLE001 — a policy we cannot introspect still counts
                refs = []
            found.append({
                "kind": kind,
                "name": fqn,
                "attached_to": sorted({str(r["REF_ENTITY_NAME"]) for r in refs}),
            })
    return found


def current(setting: str) -> str:
    try:
        return str(session.sql(f"SELECT CURRENT_{setting}()").collect()[0][0] or "")
    except Exception:  # noqa: BLE001
        return ""


def picker(label: str, options: list[str], key: str, help: str = "", default: str = "", allow_custom = True) -> str:
    """Pick from a SHOW listing, or type a name that does not exist yet.

    Written as selectbox-plus-text rather than one free-text combobox so it
    behaves the same on every Streamlit version Snowflake ships.
    """
    if not options:
        return st.text_input(label, value=default, key=f"{key}_text", help=help).strip()
    choice = st.selectbox(label, [TYPE_IT] + options, key=f"{key}_pick", help=help)
    if choice != TYPE_IT:
        return choice
    if not allow_custom:
        return ""
    return st.text_input(f"{label} (new)", value=default, key=f"{key}_new").strip()


role_now, warehouse_now = current("ROLE"), current("WAREHOUSE")

st.title("❄️ Clone a database")
st.caption(
    f"Running as **{role_now or 'unknown role'}** on **{warehouse_now or 'no warehouse'}** — "
    f"`{PROCEDURE}` executes as the caller, so this role creates the clone and "
    "makes every grant."
)

databases = show_names("SHOW DATABASES", "name")
roles = show_names("SHOW ROLES", "name")
warehouses = show_names("SHOW WAREHOUSES", "name")

# --------------------------------------------------------------- what to clone

st.subheader("Source and target")
col_src, col_tgt = st.columns(2)
with col_src:
    source_db = picker("Source database", databases, "source",
                       help="Cloned as it stands. The source is never modified.", allow_custom=False,)
with col_tgt:
    target_db = st.text_input(
        "Target database name", key="target",
        help="Created by this run. Letters, digits, _ and $ only.",
    ).strip()

# ----------------------------------------------------------------- owner role

st.subheader("Owner role")
mode_label = st.radio(
    "What is this clone for?",
    ["Development — writable sandbox", "Read-only — frozen snapshot"],
    captions=[
        "USAGE, MONITOR and CREATE SCHEMA on the database, ALL PRIVILEGES on "
        "everything in it, present and future.",
        "USAGE down the container chain and SELECT only — the owner role cannot "
        "change the data it was handed.",
    ],
)
owner_mode = "DEVELOPMENT" if mode_label.startswith("Development") else "READ_ONLY"

stem = target_db.upper() or "CLONE"
suggested_owner = f"{stem}_DEV" if owner_mode == "DEVELOPMENT" else f"{stem}_RO"

col_role, col_own = st.columns([2, 1])
with col_role:
    owner_role = picker("Owner role", roles, "owner", default=suggested_owner,
                        help="An existing role, or a new name to create.")
with col_own:
    transfer_ownership = st.checkbox(
        "Transfer OWNERSHIP", value=(owner_mode == "DEVELOPMENT"),
        help="Move ownership of the clone and everything in it to this role, so it can "
             "drop and recreate objects without the app being involved again.",
    )
if owner_mode == "READ_ONLY" and transfer_ownership:
    st.warning(
        "Ownership overrides the read-only grant set: a role can always write to, and "
        "drop, what it owns. Leave this off for a snapshot that must not drift."
    )

# ------------------------------------------------------------ read-only roles

st.subheader("Read-only roles")
st.caption(
    "Each role listed here gets USAGE down the container chain and SELECT on the "
    "clone — including on objects created in it later."
)
picked_roles = st.multiselect("Existing roles", roles, key="ro_pick") if roles else []
typed_roles = st.text_area(
    "Roles to grant read access to",
    placeholder="ANALYST_EU\nANALYST_NAM, REPORTING_RO",
    help="One per line or comma-separated. Names that do not exist yet are created when "
         "“Create roles that do not exist” is ticked.",
)
# The procedure takes one free-text field and splits it, so the multiselect is
# folded into the same string rather than travelling as a second parameter.
readonly_roles = "\n".join([*picked_roles, typed_roles])

# ------------------------------------------------------------- everything else

with st.expander("Options"):
    warehouse = picker(
        "Also grant USAGE on warehouse", warehouses, "wh",
        help="Without warehouse USAGE these roles can list objects but cannot run a "
             "query, which reads as a bug rather than a missing grant. Leave empty if "
             "they already have one.", allow_custom=False,
    )
    create_missing_roles = st.checkbox("Create roles that do not exist", value=True)
    replace_existing = st.checkbox(
        "Replace the target if it already exists", value=False,
        help="CREATE OR REPLACE DATABASE — drops the existing database, everything in "
             "it, and every grant on it.",
    )
    continue_on_error = st.checkbox(
        "Keep going if a grant fails", value=True,
        help="A grant on an object type this account does not have (Iceberg tables, or "
             "materialized views on Standard Edition) fails harmlessly. The clone "
             "itself and role creation always stop the run.",
    )
    comment = st.text_input(
        "Comment on the clone",
        value=f"Clone of {source_db.upper() or '<source>'} taken {datetime.now():%Y-%m-%d} by {role_now}.",
    )

st.divider()

# ------------------------------------------------------------------- the plan

params = (
    source_db,
    target_db,
    owner_role,
    owner_mode,
    readonly_roles,
    warehouse or "",
    create_missing_roles,
    transfer_ownership,
    replace_existing,
    continue_on_error,
    comment,
)

try:
    plan = preview(params)
    plan_error = ""
except Exception as exc:  # noqa: BLE001 — the CALL itself failed, not the plan
    plan, plan_error = {}, " ".join(str(exc).split())

if plan_error and ("unknown user-defined function" in plan_error.lower()
                   or "does not exist" in plan_error.lower()):
    
    st.error(
        f"**{PROCEDURE} is not deployed in this account.** This app is only the "
        "form; the plan and the whole grant matrix live in that procedure. Check the *sp_clone_database.sql*.\n\n"
    )
elif plan_error:
    st.error(f"Could not reach {PROCEDURE}: {plan_error}")
elif plan.get("status") == "invalid":
    st.info(plan["detail"])
else:
    steps = plan.get("steps", [])
    target_exists = target_db.upper() in {d.upper() for d in databases}
    if target_exists and not replace_existing:
        st.error(
            f"{target_db.upper()} already exists. Pick another name, or tick “Replace "
            "the target if it already exists” under Options."
        )
    elif target_exists:
        st.warning(
            f"{target_db.upper()} exists and will be **dropped and recreated**. Its "
            "Time Travel history and every grant on it go with it."
        )

    # Advisory, not a block. A governed source is a normal thing to clone; the
    # point is that the grant matrix below is blind to the governance, so the
    # clone ends up protected by grants alone rather than by grants and policy.
    policies = source_policies(source_db)
    if policies:
        listing = "\n".join(
            f"- `{p['name']}` ({p['kind']}) → "
            + (", ".join(f"`{o}`" for o in p["attached_to"])
               if p["attached_to"] else "_attached to nothing_")
            for p in policies
        )
        caution = (
            f"**{source_db.upper()} is governed by {len(policies)} "
            f"polic{'y' if len(policies) == 1 else 'ies'}, and the clone will not be "
            "governed the same way.**\n\n" + listing + "\n\n"
            "The policies are cloned and stay attached, but the plan below reaches "
            "around them: read-only roles get `SELECT` on **every table in every "
            "schema**, so a policy guarding a view leaves the base tables under it "
            "fully readable."
        )
        if transfer_ownership:
            caution += (
                "\n\n**Ownership transfer is on.** The owner cannot detach a policy "
                "without `APPLY`, but it can `CREATE OR REPLACE` the object carrying "
                "one — the replacement carries no attachment — or read the base "
                "tables directly. Against a governed source this checkbox hands over "
                "the data."
            )
        st.warning(caution)

    with st.expander(f"SQL — {len(steps)} statements"):
        st.code("\n".join(f"{step['statement']};" for step in steps), language="sql")

    blocked = target_exists and not replace_existing
    if st.button("Clone and grant", type="primary", disabled=blocked, use_container_width=True):
        with st.spinner(f"Running {len(steps)} statements…"):
            try:
                st.session_state["result"] = call_procedure(params, dry_run=False)
            except Exception as exc:  # noqa: BLE001
                st.session_state["result"] = {
                    "status": "failed", "failed": 1, "steps": [],
                    "detail": " ".join(str(exc).split()),
                }

# ----------------------------------------------------------------- last result

result = st.session_state.get("result")
if result:
    if result.get("detail"):
        st.error(result["detail"])
    elif result.get("failed"):
        st.error(f"{result['failed']} of {len(result['steps'])} statements failed.")
    else:
        st.success(f"All {len(result['steps'])} statements completed.")
        st.caption(
            "Cloning a database also copies the grants that were on the objects inside "
            "the source. If the source was shared more widely than this copy should be, "
            "check `SHOW GRANTS` on the clone and revoke what does not belong."
        )
    if result.get("steps"):
        st.dataframe(
            [
                {"#": s["step_no"], "Step": s["step"], "Status": s["status"], "Detail": s["detail"]}
                for s in result["steps"]
            ],
            use_container_width=True, hide_index=True,
        )
