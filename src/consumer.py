import os
import json
import time
from kafka import KafkaConsumer

# --- Configuration ---
# Paste your Confluent credentials here (same as producer)
CONFIG = {
    'KAFKA_BOOTSTRAP_SERVER': 'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092',
    'KAFKA_API_KEY': '2DD5LRB2GHYE7O3K',
    'KAFKA_API_SECRET': 'cflt/dKBWrnU0bx+bnXG8zZKMHy6KfRAhOCrMp83Le7uy0ba6KnG1WJqSTJI7IEQ',
    'KAFKA_TOPIC': 'lirr_realtime_updates',
    'KAFKA_CONSUMER_GROUP': 'lirr_data_savers'  # An ID for our consumer
}

# Path to the data folder
DATA_DIR = r'C:\Users\chani\OneDrive\Documents\PyhtonTraning\lirr-tracker\data'
# The 'r' before the string is important for Windows paths

# --- Kafka Consumer Setup ---
try:
    consumer = KafkaConsumer(
        CONFIG['KAFKA_TOPIC'],
        bootstrap_servers=CONFIG['KAFKA_BOOTSTRAP_SERVER'],
        sasl_plain_username=CONFIG['KAFKA_API_KEY'],
        sasl_plain_password=CONFIG['KAFKA_API_SECRET'],
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        group_id=CONFIG['KAFKA_CONSUMER_GROUP'],
        # This function deserializes the data from JSON
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='earliest'  # Start reading from the beginning of the topic
    )
    print("Kafka Consumer initialized successfully.")
except Exception as e:
    print(f"Error initializing Kafka Consumer: {e}")
    exit(1)

# --- Main Loop ---


def main():
    """
    Main loop to run the consumer. It listens for messages and
    saves them to a file.
    """
    print("Starting the LIRR data consumer...")
    print(f"Will save data to the '{DATA_DIR}' folder.")

    # Ensure the data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    try:
        # The consumer will wait here and listen for new messages
        for message in consumer:
            # When a message comes in, 'message.value' has our dictionary
            data = message.value

            # Get the current timestamp to create a unique filename
            filename = f"lirr_update_{int(time.time() * 1000)}.json"
            filepath = os.path.join(DATA_DIR, filename)

            try:
                # Save the single train update as a new JSON file
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2)

                print(
                    f"Saved message to {filepath} (Trip ID: {data.get('trip_id')})")

            except IOError as e:
                print(f"Error writing file {filepath}: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        # Clean up and close the consumer connection
        consumer.close()
        print("Kafka Consumer closed.")


if __name__ == "__main__":
    main()
