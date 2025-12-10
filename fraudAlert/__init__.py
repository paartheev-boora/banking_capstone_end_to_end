import json
import logging
from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

import azure.functions as func


# configuration constants (no secrets read here)
TXNS_CONTAINER = "ATMTransactions"
ACCOUNTS_CONTAINER = "AccountProfile"
ALERTS_CONTAINER = "FraudAlerts"

HIGH_VALUE_THRESHOLD = 50000
RAPID_WINDOW_MIN = 5
RAPID_MIN_TX = 3


# --- helper functions (stateless checks) ---
def detect_high_value(tx):
    try:
        amt = float(tx.get("TransactionAmount", 0))
        return amt >= HIGH_VALUE_THRESHOLD
    except Exception:
        return False


def detect_geo_anomaly(tx, account):
    """
    Simple distance-based anomaly: if account has LastKnownLocation and
    tx.GeoLocation exists, compute haversine distance and flag if > 500 km.
    Note: This function is best-effort and defensive against bad formats.
    """
    try:
        last_location = account.get("LastKnownLocation")
        if not last_location:
            return False

        lat1, lon1 = map(float, last_location.split(","))
        lat2, lon2 = map(float, tx.get("GeoLocation", "0,0").split(","))

        # Haversine
        R = 6371.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat/2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2) ** 2
        distance = 2 * R * asin(sqrt(a))

        return distance > 500  # km
    except Exception:
        return False


# NOTE: We DO NOT call into Cosmos SDK here. If you need to read historical txs
# to detect rapid transactions, prefer to do that in a separate function which
# can use input bindings or be a different process. Here we will assume the
# Event Grid event can include necessary context or we skip this check.
def detect_rapid_transactions_placeholder():
    # Placeholder — if you want full rapid-transaction detection using history,
    # either:
    #  - add a Cosmos DB input binding to fetch recent txns for this CustomerID, or
    #  - run an async job that enriches events with historical context before this function.
    return False


# ----------------------------
# Main function (binding-based)
# ----------------------------
def main(event: func.EventGridEvent, outputQueueItem: func.Out[str], outputDocument: func.Out[func.Document]):
    """
    event: EventGridEvent input
    outputQueueItem: ServiceBus output binding (string)
    outputDocument: CosmosDB output binding (Document)
    """

    try:
        tx = event.get_json()
    except Exception:
        logging.exception("Invalid event payload")
        return

    # Extract fields we need (defensive)
    customer_id = tx.get("CustomerID")
    transaction_id = tx.get("TransactionID") or tx.get("TxnID") or f"tx-{datetime.utcnow().timestamp()}"
    txn_timestamp = tx.get("TxnTimestamp") or tx.get("TransactionTime") or datetime.utcnow().isoformat()

    # Retrieve or assume a customer profile object is present in the event payload (best-effort)
    account_profile = tx.get("AccountProfile") or {}  # ideally you enrich event with profile earlier

    anomalies = []

    if detect_high_value(tx):
        anomalies.append("High value transaction")

    if detect_geo_anomaly(tx, account_profile):
        anomalies.append("Unusual location")

    if detect_rapid_transactions_placeholder():
        anomalies.append("Rapid transactions")

    if not anomalies:
        logging.info("No anomalies detected, skipping alert creation.")
        return

    alert_doc = {
        "id": f"alert-{transaction_id}",
        "CustomerID": customer_id,
        "TransactionID": transaction_id,
        "Anomalies": anomalies,
        "CreatedAt": datetime.utcnow().isoformat(),
        "SourceEvent": tx  # optionally store event payload for troubleshooting (be mindful of size)
    }

    # Write to Cosmos DB via output binding (no SDK)
    outputDocument.set(func.Document.from_dict(alert_doc))

    # Also push to Service Bus via output binding
    outputQueueItem.set(json.dumps(alert_doc))

    logging.info(f"Fraud alert created for txn {transaction_id}: {anomalies}")
