import json
from confluent_kafka import Producer
import time

conf = {
    'bootstrap.servers': 'localhost:19092',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'superuser',
    'sasl.password': 'secretpassword'
}


producer = Producer(conf)


def parse_line_to_json(line):
    parts = line.split()  # Split the line by spaces

    if len(parts) != 8:
        raise ValueError(f"Invalid line format: {line}")

    data = {
        "date": parts[0],  # yyyy-mm-dd
        "time": parts[1],  # hh:mm:ss.xxx
        "epoch": int(parts[2]),  # epoch as int
        "moteid": int(parts[3]),  # moteid as int
        "temperature": float(parts[4]),  # temperature as real (float)
        "humidity": float(parts[5]),  # humidity as real (float)
        "light": float(parts[6]),  # light as real (float)
        "voltage": float(parts[7])  # voltage as real (float)
    }

    return json.dumps(data)

# Function to process a file and produce messages for each line
def process_file(file_path, topic):
    with open(file_path, 'r') as file:
        for line in file:
            # Strip leading/trailing whitespace characters from the line
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            try:
                # Parse the line into JSON format
                json_data = parse_line_to_json(line)
                print(f"Producing message: {json_data}")

                # Produce JSON message to the Kafka topic
                produce_json_messages(topic, json_data)
                time.sleep(1)

            except ValueError as e:
                print(f"Error processing line: {e}")

# Optional callback for delivery reports
def delivery_callback(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Function to produce JSON messages
def produce_json_messages(topic, data):
    try:
        # Produce the message to the topic
        producer.produce(topic, data.encode('utf-8'), callback=delivery_callback)
    except BufferError:
        print("Local producer queue is full, waiting for free space.")
        producer.poll(1)  # Wait for space to be available
        produce_json_messages(topic, data)

    # Poll producer to handle delivery callbacks
    producer.poll(0)


if __name__ == "__main__":

    file_path = "./data.txt"
    topic = 'sensor_data'
    process_file(file_path, topic)


    producer.flush()
