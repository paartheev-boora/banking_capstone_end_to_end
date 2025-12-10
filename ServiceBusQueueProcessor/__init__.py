import os
import json
import pandas as pd
from io import StringIO
from datetime import datetime
from dateutil import parser as dateparser

from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from dotenv import load_dotenv
import azure.functions as func

def main(msg: func.ServiceBusMessage):
    """
    Azure Function triggered by Service Bus Queue.
    The queue message MUST contain: {"blob_url": "<url>"}
    """

    raw_body = msg.get_body().decode("utf-8")
    print("Received message:", raw_body)

    # Parse message
    try:
        body = json.loads(raw_body)
    except Exception as e:
        print("ERROR: Service Bus message is not valid JSON", e)
        return

    blob_url = body.get("blob_url")
    if not blob_url:
        print("ERROR: Message missing 'blob_url'")
        return

    # Call your existing ingestion logic
    try:
        ingest(blob_url)
        print("SUCCESS: File processed")
    except Exception as e:
        print("ERROR while processing blob:", e)
# ============================================================
# LOAD VARIABLES FROM .env FILE
# ============================================================
load_dotenv()  # loads .env in the same directory

COSMOS_ENDPOINT = os.environ["CosmosDBConnection"].split(";")[0].split("=")[1]
COSMOS_KEY      = os.environ["CosmosDBConnection"].split(";")[1].split("=")[1]
COSMOS_DB       = "OperationalDB"

STORAGE_CONN    = os.environ["AzureWebJobsStorage"]

CONTAINERS = {
    "atm": "ATMTransactions",
    "upi": "UPIEvents",
    "customers": "AccountProfile",
    "alerts": "FraudAlerts"
}
TIMESTAMP_FIELD = "TransactionTime"


# ============================================================
# SCHEMA INFERENCE & VALIDATION
# ===========================================================
def infer_schema(first_row):
    """Infer schema from the first row only."""
    schema = {"required": [], "types": {}}

    for k, v in first_row.items():
        schema["required"].append(k)

        # number?  <-- CHECK NUMBER FIRST
        try:
            float(v)
            schema["types"][k] = "number"
            continue
        except:
            pass

        # timestamp?  <-- CHECK TIMESTAMP SECOND
        try:
            dateparser.parse(str(v))
            schema["types"][k] = "timestamp"
            continue
        except:
            pass

        schema["types"][k] = "string"

    return schema



def validate_record(row, schema):
    """Validate based on inferred schema."""
    for field in schema["required"]:
        if field not in row or row[field] in ("", None):
            return False, f"Missing field: {field}"

    for field, datatype in schema["types"].items():
        value = row.get(field)

        if datatype == "timestamp":
            try:
                dateparser.parse(str(value))
            except:
                return False, f"Invalid timestamp in {field}"

        if datatype == "number":
            try:
                float(value)
            except:
                return False, f"Invalid number in {field}"

    return True, None


# ============================================================
# CLASSIFICATION + SUSPICIOUS DETECTION
# ============================================================
def classify(row, source):
    desc = str(row.get("description", "")).lower()
    amount = float(row.get("amount", 0))

    if source == "atm":
        if "withdraw" in desc or amount < 0:
            row["transaction_type"] = "ATM_WITHDRAWAL"
        elif "deposit" in desc or amount > 0:
            row["transaction_type"] = "ATM_DEPOSIT"
        else:
            row["transaction_type"] = "ATM_OTHER"

    elif source == "upi":
        if "pay" in desc or "debit" in desc:
            row["transaction_type"] = "UPI_PAYMENT"
        else:
            row["transaction_type"] = "UPI_OTHER"

    return row


def high_value_suspicious(rec):
    amt = abs(float(rec.get("amount", 0)))
    t = rec.get("transaction_type", "")

    if "ATM" in t and amt >= 20000:
        return True
    if "UPI" in t and amt >= 50000:
        return True
    return False


def rapid_withdrawals(records):
    withdrawals = [
        r for r in records if r.get("transaction_type") == "ATM_WITHDRAWAL"
    ]

    # Sort by correct timestamp field
    withdrawals.sort(key=lambda r: dateparser.parse(r["TransactionTime"]))

    alerts = []
    WINDOW_MIN = 5
    MIN_TX = 3

    for i in range(len(withdrawals)):
        group = [withdrawals[i]]
        t0 = dateparser.parse(withdrawals[i]["TransactionTime"])

        for j in range(i + 1, len(withdrawals)):
            t1 = dateparser.parse(withdrawals[j]["TransactionTime"])
            if (t1 - t0).total_seconds() / 60 <= WINDOW_MIN:
                group.append(withdrawals[j])

        if len(group) >= MIN_TX:
            alerts.append({
                "id": f"rapid-{withdrawals[i]['AccountNumber']}-{withdrawals[i]['TransactionTime']}",
                "AlertType": "RAPID_WITHDRAWALS",
                "AccountNumber": withdrawals[i]["AccountNumber"],
                "Count": len(group),
                "StartTime": group[0]["TransactionTime"],
                "EndTime": group[-1]["TransactionTime"]
            })

    return alerts



# ============================================================
# CORRECTED BLOB DOWNLOAD
# ============================================================
def download_blob_text(blob_url):
    """
    Handles nested folder paths correctly.
    Works for URLs like:
    https://account.blob.core.windows.net/raw/atm/file.csv
    """
    bsc = BlobServiceClient.from_connection_string(STORAGE_CONN)

    # Extract the part after ".net/"
    path = blob_url.split(".net/")[1]          # raw/atm/file.csv
    parts = path.split("/")

    container = parts[0]                       # raw
    blob_path = "/".join(parts[1:])            # atm/file.csv

    blob = bsc.get_container_client(container).get_blob_client(blob_path)
    return blob.download_blob().content_as_text()


# ============================================================
# MAIN INGEST FUNCTION
# ============================================================
def ingest(blob_url):
    print(f"\nProcessing file: {blob_url}")

    # Determine source type
    if "atm" in blob_url.lower():
        source = "atm"
    elif "upi" in blob_url.lower():
        source = "upi"
    else:
        source = "customers"

    raw_text = download_blob_text(blob_url)

    # ---------------- JSONL ----------------
    if blob_url.endswith(".jsonl"):
        lines = raw_text.splitlines()
        first = json.loads(lines[0])
        schema = infer_schema(first)

        records = []
        seen = set()

        for line in lines:
            obj = json.loads(line)
            uid = obj.get("TransactionID") or obj.get("CustomerID")

            if not uid or uid in seen:
                continue
            seen.add(uid)

            ok, err = validate_record(obj, schema)
            if not ok:
                continue

            obj["id"] = uid
            obj = classify(obj, source)
            records.append(obj)

    # ---------------- CSV ----------------
    else:
        df = pd.read_csv(StringIO(raw_text))
        schema = infer_schema(df.iloc[0].to_dict())

        records = []
        seen = set()

        for _, row in df.iterrows():
            obj = row.to_dict()
            uid = obj.get("CustomerID")

            if not uid or uid in seen:
                continue
            seen.add(uid)

            ok, err = validate_record(obj, schema)
            if not ok:
                continue

            obj["id"] = uid
            records.append(obj)

    # ============================================================
    # WRITE TO COSMOS DB
    # ============================================================
    cosmos = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    db = cosmos.get_database_client(COSMOS_DB)

    main_container = db.get_container_client(CONTAINERS[source])
    alert_container = db.get_container_client(CONTAINERS["alerts"])

    # Insert each record
    for rec in records:
        main_container.upsert_item(rec)

        if source != "customers" and high_value_suspicious(rec):
            alert_container.upsert_item({
                "id": rec["id"] + "-high",
                "AlertType": "HIGH_VALUE",
                "Details": rec
            })

    # Rapid withdrawal detection for ATM
    if source == "atm":
        alerts = rapid_withdrawals(records)
        for a in alerts:
            alert_container.upsert_item(a)

    # Customer360 for customer files
    if source == "customers":
        for c in records:
            main_container.upsert_item({
                "id": c["CustomerID"],
                "CustomerID": c["CustomerID"],
                "Name": c.get("Name"),
                "Phone": c.get("Phone"),
                "Email": c.get("Email"),
                "CreatedAt": datetime.utcnow().isoformat()
            })

    print("Done!\n")


