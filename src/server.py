import json
import sqlite3
import zlib  
import threading
import time
from confluent_kafka import Consumer, KafkaException, KafkaError


def compress_data(data):
    # Convert data to string and compress it
    data_str = json.dumps(data)
    compressed_data = zlib.compress(data_str.encode('utf-8'))
    return compressed_data

# Define decompression
def decompress_data(compressed_data):
    decompressed_data = zlib.decompress(compressed_data).decode('utf-8')
    return json.loads(decompressed_data)

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'superuser',
    'sasl.password': 'secretpassword'
}


consumer = Consumer(consumer_conf)
consumer.subscribe(['sensor_data'])


conn = sqlite3.connect('data_comparison.db')
cursor = conn.cursor()

# Create a table to store original, compressed, and decompressed data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS data_comparison (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        original_data TEXT,  -- Original data in JSON format
        compressed_data TEXT,  -- Compressed data
        decompressed_data TEXT  -- Decompressed data in JSON format
    )
''')
conn.commit()

# Shared storage for compressed data
data_buffer = []

# Lock for thread synchronization
data_lock = threading.Lock()

# Function to consume messages from Kafka
def consume_messages():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No message received
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition {msg.partition()} at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                try:
                    message_value = msg.value().decode('utf-8')
                    data = json.loads(message_value)
                    

                    with data_lock:
                        data_buffer.append(data)

                except json.JSONDecodeError:
                    print(f"Received non-JSON message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass  # Clean exit on Ctrl+C
    finally:
        # Close the consumer
        consumer.close()

# Function to compress, decompress, and store data every second
def compress_decompress_and_store():
    while True:
        time.sleep(1)  # Wait for 1 second

        with data_lock:
            if data_buffer:
                for data in data_buffer:
                    # Get the current timestamp
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                    
                    # Compress data
                    compressed_data = compress_data(data)

                    # Decompress the data to compare integrity
                    decompressed_data = decompress_data(compressed_data)

                    # Insert original, compressed, and decompressed data into the database
                    cursor.execute('''
                        INSERT INTO data_comparison (timestamp, original_data, compressed_data, decompressed_data)
                        VALUES (?, ?, ?, ?)
                    ''', (timestamp, json.dumps(data), compressed_data, json.dumps(decompressed_data)))

                    conn.commit()
                
                # Clear the buffer after storing
                data_buffer.clear()

# Main function to start the consumer and compression/decompression process
if __name__ == "__main__":
    # Create and start the consumer thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Start compression and decompression in the main thread
    compress_decompress_and_store()

    # Wait for the consumer thread to finish (optional)
    consumer_thread.join()
