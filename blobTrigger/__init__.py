import json
import logging
import azure.functions as func

def main(event: func.EventGridEvent, outputQueueItem: func.Out[str]):
    logging.info("EventGrid Trigger executed")

    data = event.get_json()
    blob_url = data.get("url") or data["data"].get("url")

    payload = {
        "blob_url": blob_url,
        "event_time": str(event.event_time),
        "validated": True
    }

    outputQueueItem.set(json.dumps(payload))
    logging.info("Message sent to Service Bus queue ingestionqueue")
