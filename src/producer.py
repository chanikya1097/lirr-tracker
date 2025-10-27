import time
import json
import requests
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer

# --- Configuration ---
# Paste your Confluent credentials here
# MTA API Key is NO LONGER NEEDED
CONFIG = {
    'KAFKA_BOOTSTRAP_SERVER': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
    'KAFKA_API_KEY': '2DD5LRB2GHYE7O3K',
    'KAFKA_API_SECRET': 'cflt/dKBWrnU0bx+bnXG8zZKMHy6KfRAhOCrMp83Le7uy0ba6KnG1WJqSTJI7IEQ',
    'KAFKA_TOPIC': 'lirr_realtime_updates'
}

# The URL for the LIRR GTFS Realtime feed
LIRR_URL = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/lirr%2Fgtfs-lirr'

# --- Kafka Producer Setup ---
# Initialize the Kafka producer
try:
    producer = KafkaProducer(
        bootstrap_servers=CONFIG['KAFKA_BOOTSTRAP_SERVER'],
        sasl_plain_username=CONFIG['KAFKA_API_KEY'],
        sasl_plain_password=CONFIG['KAFKA_API_SECRET'],
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer initialized successfully.")
except Exception as e:
    print(f"Error initializing Kafka Producer: {e}")
    exit(1)  # Exit the script if we can't connect

# --- Functions ---


def fetch_mta_data():
    """
    Fetches the real-time data from the MTA LIRR API.
    Returns the raw binary content if successful, None otherwise.
    """
    try:
        # No API key is needed anymore
        response = requests.get(LIRR_URL, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        print(
            f"Successfully fetched MTA data ({len(response.content)} bytes).")
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching MTA data: {e}")
        return None


def parse_gtfs_data(binary_data):
    """
    Parses the raw GTFS binary data into a list of vehicle positions.
    Returns a list of dictionaries, where each dict is a train update.
    """
    feed = gtfs_realtime_pb2.FeedMessage()  # pylint: disable=no-member
    feed.ParseFromString(binary_data)

    train_updates = []

    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle

            update = {
                'trip_id': vehicle.trip.trip_id,
                'route_id': vehicle.trip.route_id,
                'vehicle_id': vehicle.vehicle.id,
                'latitude': vehicle.position.latitude,
                'longitude': vehicle.position.longitude,
                'current_status': gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status),  # pylint: disable=no-member
                'timestamp': vehicle.timestamp,
                'stop_id': vehicle.stop_id
            }
            train_updates.append(update)

    return train_updates


def send_to_kafka(topic, data_list):
    """
    Sends a list of train updates to the Kafka topic.
    """
    if not data_list:
        print("No train updates to send.")
        return

    print(f"Sending {len(data_list)} train updates to Kafka...")
    for update in data_list:
        try:
            key = update.get('trip_id', 'unknown').encode('utf-8')
            producer.send(topic, key=key, value=update)
        except Exception as e:
            print(f"Error sending message to Kafka: {e}")

    producer.flush()
    print("All messages flushed to Kafka.")

# --- Main Loop ---


def main():
    """
    Main function to run the producer ONCE.
    It fetches, parses, and sends data, then stops.
    Jenkins will be responsible for running this script on a schedule.
    """
    print("Starting the LIRR data producer run...")
    raw_data = fetch_mta_data()

    if raw_data:
        train_updates = parse_gtfs_data(raw_data)
        send_to_kafka(CONFIG['KAFKA_TOPIC'], train_updates)

    print("\n--- Producer run finished. ---")
    # We remove the while True loop and the time.sleep()


if __name__ == "__main__":
    main()
