import json
import logging
import os

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Data generator ───────────────────────────────────────────
from data import generate_uber_ride_confirmation

# ── Environment config ───────────────────────────────────────
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME     = os.getenv("EVENT_HUBNAME")


# ─────────────────────────────────────────────────────────────
#  SEND TO EVENT HUB
# ─────────────────────────────────────────────────────────────
def send_to_event_hub(ride_data: dict) -> bool:
    """
    Serialise ride_data to JSON and publish it to Azure Event Hub.

    Returns True on success, False on failure.
    """

    # ✅ Guard against None or empty payload
    if not ride_data:
        logger.warning("send_to_event_hub called with empty ride_data — skipping.")
        return False

    try:
        # ✅ Use context manager — guarantees producer.close()
        #    even if send_batch() raises an exception
        with EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME,
        ) as producer:

            # Serialise dict → JSON string
            # default=str handles any edge-case non-serialisable values
            ride_json = json.dumps(ride_data, default=str)

            # Create batch and add the single event
            event_batch = producer.create_batch()
            event_batch.add(EventData(ride_json))

            # Publish to Event Hub
            producer.send_batch(event_batch)

        logger.info(
            "Event sent  ride_id=%s  confirmation=%s",
            ride_data.get("ride_id", "unknown"),
            ride_data.get("confirmation_number", "unknown"),
        )
        return True                                # ✅ consistent bool return

    except Exception as e:
        logger.error("Failed to send event to Event Hub: %s", str(e))
        return False                               # ✅ consistent bool return


# ─────────────────────────────────────────────────────────────
#  QUICK TEST
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 70)
    print("GENERATING SINGLE RIDE")
    print("=" * 70)
    ride = generate_uber_ride_confirmation()
    print(json.dumps(ride, indent=2, default=str))

    print("\n" + "=" * 70)
    print("SENDING TO EVENT HUB")
    print("=" * 70)
    success = send_to_event_hub(ride)
    print(f"Result: {'✅ Sent successfully' if success else '❌ Failed — check logs'}")