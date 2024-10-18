import json
import sqlite3
import zlib  
import threading
import time
from MultiSensorDataGrouper import MultiSensorDataGrouper  , load_data
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
        
        self.file_path = 'data_test.txt'
        self.epsilon = 0.5
        self.grouper = MultiSensorDataGrouper(epsilon=self.epsilon, window_size=100)
        
        self.base_moteid = 1  # Use moteid 1 as the base
        self.attribute = 'humidity'  # Attribute to analyze

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
        last_written_time = time.time()  # Keep track of the last time data was written
        
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
                        # Decode the message and append to the buffer
                        message_value = msg.value().decode('utf-8')
                        data = json.loads(message_value)

                        with self.data_lock:
                            self.data_buffer.append(data)

                    except json.JSONDecodeError:
                        print(f"Received non-JSON message: {msg.value().decode('utf-8')}")

                # Check if one second has passed since the last file write
                current_time = time.time()
                if current_time - last_written_time >= 1:
                    with self.data_lock:
                        if self.data_buffer:
                            # Write data to the file in CSV-compatible format
                            with open(self.file_path, 'a') as f:
                                for data in self.data_buffer:
                                    # Ensure the fields are written as space-separated values
                                    f.write(f"{data['date']} {data['time']} {data['epoch']} "
                                            f"{data['moteid']} {data['temperature']} "
                                            f"{data['humidity']} {data['light']} {data['voltage']}\n")
                            # Clear the buffer after writing
                            self.data_buffer.clear()

                        # Update the last written time
                        last_written_time = current_time

        except KeyboardInterrupt:
            pass  # Clean exit on Ctrl+C
        finally:
            self.consumer.close()

    def compress_decompress_and_store(self):
        while True:
            time.sleep(1)  # Wait for 1 second

            with self.data_lock:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                
                df = load_data(self.file_path)
                # Compress and decompress data
                base_signal, other_signals, timestamps = self.grouper.extract_signals(df, self.base_moteid, self.attribute)
                base_signals, ratio_signals, total_memory_cost = self.grouper.static_group(df, self.base_moteid, self.attribute)
                # decompressed_data = self.decompress_data(compressed_data)
                reconstructed_base_signal = self.grouper.reconstruct_signal(base_signals[0][1])
                
                # Reconstruct the other signals from their ratio buckets
                reconstructed_other_signals = []
                for moteid, ratio_buckets in ratio_signals.items():
                    reconstructed_signal = self.grouper.reconstruct_signal(ratio_buckets, base_signal)
                    reconstructed_other_signals.append(reconstructed_signal)

                # Step 4: Plot original vs reconstructed signals

                # Combine the base signal and other signals into one list for plotting
                original_signals = [base_signal] + other_signals
                reconstructed_signals = [reconstructed_base_signal] + reconstructed_other_signals
                
                print(f'Total memory cost after compression: {total_memory_cost} buckets')
                
                self.grouper.plot_signals_single(original_signals, reconstructed_signals)


    
    # def compress_decompress_and_store(self):
    #     while True:
    #         time.sleep(1)  # Wait for 1 second

    #         with self.data_lock:
    #             if self.data_buffer:
    #                 for data in self.data_buffer:
    #                     timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                        
    #                     # Compress and decompress data
    #                     compressed_data = self.compress_data(data)
    #                     decompressed_data = self.decompress_data(compressed_data)

    #                     # Insert into the database
    #                     self.cursor.execute('''
    #                         INSERT INTO data_comparison (timestamp, original_data, compressed_data, decompressed_data)
    #                         VALUES (?, ?, ?, ?)
    #                     ''', (timestamp, json.dumps(data), compressed_data, json.dumps(decompressed_data)))
    #                     self.conn.commit()
                    
    #                 # Clear the buffer after storing
    #                 self.data_buffer.clear()

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
