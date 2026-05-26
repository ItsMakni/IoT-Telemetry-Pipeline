import os
import json
import time
import queue
import threading
import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = "energy/+/+/telemetry" # Wildcard to subscribe to all telemetry

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "iot_telemetry")
DB_USER = os.getenv("DB_USER", "iot_user")
DB_PASS = os.getenv("DB_PASS", "iot_password")

# Thread-safe queue for batching
message_queue = queue.Queue()
BATCH_SIZE = 100
BATCH_TIMEOUT = 5 # seconds

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(psycopg2.OperationalError)
)
def get_db_connection():
    print(f"Connecting to database {DB_NAME} at {DB_HOST}...")
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker at {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        # Add to batching queue
        message_queue.put(payload)
    except Exception as e:
        print(f"Error parsing message: {e}")

def db_writer_worker():
    conn = get_db_connection()
    conn.autocommit = True
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO device_telemetry (
        timestamp, factory_id, machine_id, voltage, current, power_factor, temperature, kwh
    ) VALUES %s
    """
    
    batch = []
    last_flush = time.time()

    while True:
        try:
            # Non-blocking get with a small timeout
            payload = message_queue.get(timeout=1)
            
            # Map JSON to tuple
            data_tuple = (
                payload.get('timestamp'),
                payload.get('factory_id'),
                payload.get('machine_id'),
                payload.get('voltage'),
                payload.get('current'),
                payload.get('power_factor'),
                payload.get('temperature'),
                payload.get('kwh')
            )
            batch.append(data_tuple)
            
        except queue.Empty:
            pass # Timeout reached, check if we need to flush anyway

        now = time.time()
        # Flush if we hit batch size or timeout
        if len(batch) >= BATCH_SIZE or (len(batch) > 0 and (now - last_flush) >= BATCH_TIMEOUT):
            try:
                psycopg2.extras.execute_values(cursor, insert_query, batch)
                print(f"Successfully bulk inserted {len(batch)} records into PostgreSQL.")
            except Exception as e:
                print(f"Database insertion error: {e}")
                # Try to reconnect if connection dropped
                try:
                    conn = get_db_connection()
                    conn.autocommit = True
                    cursor = conn.cursor()
                except Exception as conn_e:
                    print(f"Could not reconnect to DB: {conn_e}")
            finally:
                batch = []
                last_flush = time.time()

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(Exception)
)
def connect_mqtt():
    client = mqtt.Client(client_id="ingestion_service")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    return client

if __name__ == '__main__':
    print("Starting Ingestion Service...")
    
    # Start background database writer thread
    writer_thread = threading.Thread(target=db_writer_worker, daemon=True)
    writer_thread.start()

    # Start MQTT client
    mqtt_client = connect_mqtt()
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("Ingestion service stopped.")
        mqtt_client.disconnect()
