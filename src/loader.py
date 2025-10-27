import mysql.connector
from mysql.connector import errorcode
import os
import json
import shutil  # We'll use this to move files

# --- MySQL Configuration ---
# Fill this in with your local MySQL credentials
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'Chanikya15241*',
    'host': '127.0.0.1',  # This means 'localhost'
    'database': 'lirr_db'   # The database you created
}

TABLE_NAME = 'raw_lirr_updates'
DATA_DIR = r'C:\Users\chani\OneDrive\Documents\PyhtonTraning\lirr-tracker\data'
# The 'r' before the string is important for Windows paths
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')

# The SQL command to create our table
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    trip_id VARCHAR(255),
    route_id VARCHAR(255),
    vehicle_id VARCHAR(255),
    latitude DOUBLE,
    longitude DOUBLE,
    current_status VARCHAR(255),
    timestamp BIGINT,
    stop_id VARCHAR(255),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
"""

# The SQL command to insert one row of data
INSERT_SQL = f"""
INSERT INTO {TABLE_NAME} 
    (trip_id, route_id, vehicle_id, latitude, longitude, current_status, timestamp, stop_id) 
VALUES 
    (%(trip_id)s, %(route_id)s, %(vehicle_id)s, %(latitude)s, %(longitude)s, %(current_status)s, %(timestamp)s, %(stop_id)s);
"""


def create_table(cursor):
    """Creates the table if it doesn't exist."""
    try:
        print(f"Checking table '{TABLE_NAME}'...")
        cursor.execute(CREATE_TABLE_SQL)
        print("Table check complete (created or already exists).")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")
        raise  # Stop the script if we can't create the table


def load_data_to_mysql():
    """
    Scans the data/ folder, loads .json files into MySQL,
    and moves them to data/processed.
    """

    # Ensure the processed directory exists
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    try:
        print(f"Connecting to MySQL database '{MYSQL_CONFIG['database']}'...")
        cnx = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = cnx.cursor()
        print("Connection successful.")

        # Make sure the table exists before we try to insert data
        create_table(cursor)

        # Get a list of all .json files in the data directory
        files_to_process = [f for f in os.listdir(
            DATA_DIR) if f.endswith('.json')]

        if not files_to_process:
            print("No new files to process.")
            return

        print(f"Found {len(files_to_process)} new files to load...")

        for filename in files_to_process:
            filepath = os.path.join(DATA_DIR, filename)

            try:
                # 1. Read the JSON file
                with open(filepath, 'r') as f:
                    data = json.load(f)

                # 2. Insert the data into MySQL
                # The 'data' dictionary keys must match the %s placeholders
                cursor.execute(INSERT_SQL, data)

                # 3. Move the processed file
                shutil.move(filepath, os.path.join(PROCESSED_DIR, filename))

                print(f"Successfully loaded and moved: {filename}")

            except json.JSONDecodeError:
                print(f"Error: Could not read {filename}. Skipping.")
            except mysql.connector.Error as err:
                print(f"Error inserting data for {filename}: {err}")
            except Exception as e:
                print(f"An unexpected error occurred with {filename}: {e}")

        # Commit all the successful inserts to the database
        cnx.commit()
        print("All changes committed to MySQL.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database '{MYSQL_CONFIG['database']}' does not exist")
        else:
            print(err)
    finally:
        # Clean up
        if 'cnx' in locals() and cnx.is_connected():
            cursor.close()
            cnx.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    load_data_to_mysql()
