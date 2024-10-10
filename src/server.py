import json
import sqlite3
import zlib  
import threading
import time
from confluent_kafka import Consumer, KafkaException, KafkaError


class DataProcessor:
    def __init__(self, kafka_config, db_path, topic):
        self.kafka_config = kafka_config
        self.topic = topic
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe([self.topic])
        
        # SQLite connection
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

        # Shared buffer for holding data between threads
        self.data_buffer = []
        self.data_lock = threading.Lock()

        # Initialize DB table
        self._initialize_db()

    def _initialize_db(self):
        # Create a table for original, compressed, and decompressed data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS data_comparison (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                original_data TEXT,  -- Original data in JSON format
                compressed_data BLOB,  -- Compressed data as BLOB
                decompressed_data TEXT  -- Decompressed data in JSON format
            )
        ''')
        self.conn.commit()

    def compress_data(self, data):
        # Convert data to string and compress it
        data_str = json.dumps(data)
        compressed_data = zlib.compress(data_str.encode('utf-8'))
        return compressed_data

    def decompress_data(self, compressed_data):
        decompressed_data = zlib.decompress(compressed_data).decode('utf-8')
        return json.loads(decompressed_data)

    def consume_messages(self):
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

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

                        with self.data_lock:
                            self.data_buffer.append(data)

                    except json.JSONDecodeError:
                        print(f"Received non-JSON message: {msg.value().decode('utf-8')}")

        except KeyboardInterrupt:
            pass  # Clean exit on Ctrl+C
        finally:
            self.consumer.close()

    def compress_decompress_and_store(self):
        while True:
            time.sleep(1)  # Wait for 1 second

            with self.data_lock:
                if self.data_buffer:
                    for data in self.data_buffer:
                        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                        
                        # Compress and decompress data
                        compressed_data = self.compress_data(data)
                        decompressed_data = self.decompress_data(compressed_data)

                        # Insert into the database
                        self.cursor.execute('''
                            INSERT INTO data_comparison (timestamp, original_data, compressed_data, decompressed_data)
                            VALUES (?, ?, ?, ?)
                        ''', (timestamp, json.dumps(data), compressed_data, json.dumps(decompressed_data)))
                        self.conn.commit()
                    
                    # Clear the buffer after storing
                    self.data_buffer.clear()

    def start(self):
        # Create and start the consumer thread
        consumer_thread = threading.Thread(target=self.consume_messages)
        consumer_thread.start()

        # Run the compression/decompression loop in the main thread
        self.compress_decompress_and_store()

        # Wait for the consumer thread to finish (optional)
        consumer_thread.join()

    def __del__(self):
        # Clean up SQLite connection
        self.conn.close()


if __name__ == "__main__":
    kafka_conf = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': 'superuser',
        'sasl.password': 'secretpassword'
    }

    db_path = 'data_comparison.db'
    topic = 'sensor_data'

    processor = DataProcessor(kafka_conf, db_path, topic)
    processor.start()
