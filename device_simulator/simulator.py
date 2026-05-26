import os
import time
import json
import random
from datetime import datetime, timezone
import pandas as pd
import paho.mqtt.client as mqtt
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

# Configuration from environment variables
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
DATA_FILE = os.getenv("DATA_FILE", "/app/data/telemetry.csv")
FACTORY_ID = os.getenv("FACTORY_ID", "factory_alpha")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Connected to MQTT Broker at {MQTT_BROKER}")
    else:
        print(f"Failed to connect, return code {rc}")

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(Exception)
)
def connect_mqtt():
    client = mqtt.Client(client_id="device_simulator")
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    return client

def generate_mock_row(machine_id):
    """Fallback if CSV is not found, to keep the simulator running."""
    return {
        "Machine_ID": machine_id,
        "Voltage": random.uniform(215.0, 235.0),
        "Current": random.uniform(10.0, 50.0),
        "Power_Factor": random.uniform(0.85, 0.99),
        "Temperature": random.uniform(30.0, 70.0),
        "kWh": random.uniform(100.0, 500.0)
    }

def run_simulation(client):
    has_data = os.path.exists(DATA_FILE)
    if has_data:
        try:
            print(f"Loading data from {DATA_FILE}...")
            df = pd.read_csv(DATA_FILE)
            print(f"Loaded {len(df)} rows.")
        except Exception as e:
            print(f"Failed to read CSV: {e}")
            has_data = False
    
    if not has_data:
        print("Running with randomly generated mock data since CSV was not found.")

    row_index = 0
    machines = ["machine_01", "machine_02", "machine_03", "machine_04", "machine_05"]

    while True:
        try:
            if has_data:
                row = df.iloc[row_index]
                # Try to map columns resiliently
                machine_id = str(row.get("Machine_ID", random.choice(machines)))
                voltage = float(row.get("Voltage", random.uniform(220, 240)))
                current = float(row.get("Current", random.uniform(10, 50)))
                pf = float(row.get("Power_Factor", random.uniform(0.85, 0.99)))
                temp = float(row.get("Temperature", random.uniform(40, 80)))
                kwh = float(row.get("kWh", random.uniform(100, 500)))
                
                row_index = (row_index + 1) % len(df)
            else:
                machine_id = random.choice(machines)
                mock = generate_mock_row(machine_id)
                voltage = mock["Voltage"]
                current = mock["Current"]
                pf = mock["Power_Factor"]
                temp = mock["Temperature"]
                kwh = mock["kWh"]

            # Generate UTC ISO-8601 Timestamp at time of 'read'
            read_time = datetime.now(timezone.utc).isoformat()

            payload = {
                "timestamp": read_time,
                "factory_id": FACTORY_ID,
                "machine_id": machine_id,
                "voltage": voltage,
                "current": current,
                "power_factor": pf,
                "temperature": temp,
                "kwh": kwh
            }

            # Hierarchical Topic
            topic = f"energy/{FACTORY_ID}/{machine_id}/telemetry"

            # Publish
            client.publish(topic, json.dumps(payload))
            print(f"Published to {topic}: {payload}")

            # Sleep to simulate interval (e.g. 1 second per row)
            time.sleep(1)

        except Exception as e:
            print(f"Error during simulation loop: {e}")
            time.sleep(5)

if __name__ == '__main__':
    print("Starting Device Simulator...")
    mqtt_client = connect_mqtt()
    mqtt_client.loop_start()  # Start background thread for MQTT
    try:
        run_simulation(mqtt_client)
    except KeyboardInterrupt:
        print("Simulator stopped.")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
