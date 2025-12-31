import snowflake.connector
import pandas as pd
from pathlib import Path
import smtplib
from email.message import EmailMessage

# ------------------- CONFIG -------------------
SNOWFLAKE_CONFIG = {
    "user": "shubhi123",
    "password": "Shubhangi@12345",
    "account": "IEFGLJG-KG58098",   # keep as-is since it works for you
    "warehouse": "COMPUTE_WH",
    "database": "GIT_COLLAB",
    "schema": "GIT_SCHEMA"
}

EMAIL_CONFIG = {
    "sender": "shubhangi.sanjay2010@gmail.com",
    "receiver": "shubhangi.sanjay@accenture.com",
    "password": "ascpruzkntwkjses"  # Gmail App Password
}

# ------------------- STATE TRACKING (STEP 2) -------------------
execution_status = {}   # sql_file -> SUCCESS | FAILED | SKIPPED

# ------------------- CONNECT -------------------
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

# ------------------- EXECUTE SQL FILES -------------------
df = None  # IMPORTANT: initialize

sql_dir = Path("sql")  # correct folder

for sql_file in sorted(sql_dir.glob("*.sql")):
    print(f"Executing: {sql_file.name}")

    with open(sql_file) as f:
        query = f.read().strip()

    try:
        cursor.execute(query)
        execution_status[sql_file.name] = "SUCCESS"

        # Capture SELECT result
        if query.lower().startswith("select"):
            df = cursor.fetch_pandas_all()

    except Exception as e:
        execution_status[sql_file.name] = "FAILED"
        print(f"❌ Failed: {sql_file.name}")
        print(str(e))
        break   # stop execution for now (dependency logic comes next)

# ------------------- EXECUTION SUMMARY -------------------
print("Execution summary:", execution_status)

# ------------------- SAFETY CHECK -------------------
if df is None:
    raise RuntimeError("No SELECT query executed. Cannot generate Excel.")

# ------------------- SAVE RESULT -------------------
output_file = "employee_data.xlsx"
df.to_excel(output_file, index=False)

# ------------------- SEND EMAIL -------------------
msg = EmailMessage()
msg["Subject"] = "Employee Report from Snowflake"
msg["From"] = EMAIL_CONFIG["sender"]
msg["To"] = EMAIL_CONFIG["receiver"]

msg.set_content(
    f"""
Pipeline Execution Summary:

{execution_status}

Attached is the employee data extracted from Snowflake.
"""
)

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

print("✅ Pipeline executed successfully")
