import snowflake.connector
import pandas as pd
from pathlib import Path
import smtplib
from email.message import EmailMessage
import json

# ------------------- CONFIG -------------------
SNOWFLAKE_CONFIG = {
    "user": "shubhi123",
    "password": "Shubhangi@12345",        # move to env vars later
    "account": "IEFGLJG-KG58098",
    "warehouse": "COMPUTE_WH",
    "database": "GIT_COLLAB",
    "schema": "GIT_SCHEMA"
}

EMAIL_CONFIG = {
    "sender": "shubhangi.sanjay2010@gmail.com",
    "receiver": "shubhangi.sanjay@accenture.com",
    "password": "ascpruzkntwkjses"         # Gmail App Password
}

# ------------------- PATHS -------------------
sql_dir = Path("sql")
metadata_file = sql_dir / "metadata.json"

# ------------------- LOAD METADATA -------------------
with open(metadata_file) as f:
    metadata = json.load(f)

# ------------------- STATE TRACKING -------------------
execution_status = {}   # script.sql -> SUCCESS | FAILED | SKIPPED
df = None               # for SELECT output

# ------------------- DEPENDENCY CHECK -------------------
def dependencies_satisfied(script_name):
    deps = metadata.get(script_name, {}).get("depends_on", [])
    return all(execution_status.get(d) == "SUCCESS" for d in deps)

# ------------------- CONNECT TO SNOWFLAKE -------------------
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

# ------------------- EXECUTE SCRIPTS -------------------
for sql_file in sorted(sql_dir.glob("*.sql")):
    script_name = sql_file.name

    if script_name == "metadata.json":
        continue

    if not dependencies_satisfied(script_name):
        execution_status[script_name] = "SKIPPED"
        print(f"Skipped: {script_name} (dependency not satisfied)")
        continue

    print(f"Executing: {script_name}")

    try:
        query = sql_file.read_text().strip()
        cursor.execute(query)
        execution_status[script_name] = "SUCCESS"

        if query.lower().startswith("select"):
            df = cursor.fetch_pandas_all()

    except Exception as e:
        execution_status[script_name] = "FAILED"
        print(f"Failed: {script_name}")
        print(str(e))

        rollback_file = sql_dir / f"rollback_{script_name}"
        if rollback_file.exists():
            print(f"Running rollback: {rollback_file.name}")
            cursor.execute(rollback_file.read_text())

        # continue execution (agentic behavior)

# ------------------- EXECUTION SUMMARY -------------------
print("\nExecution Summary:")
for k, v in execution_status.items():
    print(f"{k}: {v}")

# ------------------- SAVE SELECT RESULT -------------------
output_file = None
if df is not None:
    output_file = "employee_data.xlsx"
    df.to_excel(output_file, index=False)

# ------------------- EMAIL SUBJECT -------------------
failed_scripts = [k for k, v in execution_status.items() if v == "FAILED"]
subject = "Snowflake Pipeline Failed" if failed_scripts else "Snowflake Pipeline Succeeded"

# ------------------- SEND EMAIL -------------------
msg = EmailMessage()
msg["Subject"] = subject
msg["From"] = EMAIL_CONFIG["sender"]
msg["To"] = EMAIL_CONFIG["receiver"]

msg.set_content(
    f"""
Snowflake Pipeline Execution Summary:

{execution_status}

Failed Scripts:
{failed_scripts if failed_scripts else 'None'}

Please find the attached report if generated.
"""
)

if output_file:
    with open(output_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=output_file
        )

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
    smtp.login(EMAIL_CONFIG["sender"], EMAIL_CONFIG["password"])
    smtp.send_message(msg)

print("Pipeline completed successfully")
