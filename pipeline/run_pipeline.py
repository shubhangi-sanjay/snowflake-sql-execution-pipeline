import snowflake.connector
import pandas as pd
from pathlib import Path
import smtplib
from email.message import EmailMessage

# ------------------- CONFIG -------------------
SNOWFLAKE_CONFIG = {
    "user": "shubhi123",
    "password": "Shubhangi@12345",
    "account": "KG58098",
    "warehouse": "COMPUTE_WH",
    "database": "GIT_COLLAB",
    "schema": "GIT_SCHEMA"
}

EMAIL_CONFIG = {
    "sender": "shubhangi.sanjay@accenture.com",
    "receiver": "shubhangi.sanjay@accenture.com",
    "password": "APP_PASSWORD"
}

# ------------------- CONNECT -------------------
conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
cursor = conn.cursor()

# ------------------- EXECUTE SQL FILES -------------------
sql_dir = Path("../sql")

for sql_file in sorted(sql_dir.glob("*.sql")):
    with open(sql_file) as f:
        query = f.read().strip()

    print(f"Executing: {sql_file.name}")
    cursor.execute(query)

    # Capture SELECT result
    if query.lower().startswith("select"):
        df = cursor.fetch_pandas_all()

# ------------------- SAVE RESULT -------------------
output_file = "employee_data.xlsx"
df.to_excel(output_file, index=False)

# ------------------- SEND EMAIL -------------------
msg = EmailMessage()
msg["Subject"] = "Employee Report from Snowflake"
msg["From"] = EMAIL_CONFIG["sender"]
msg["To"] = EMAIL_CONFIG["receiver"]
msg.set_content("Attached is the employee data extracted from Snowflake.")

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

print("Pipeline executed successfully")
