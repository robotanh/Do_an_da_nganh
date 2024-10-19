import json
import threading
import time
from MultiSensorDataGrouper import MultiSensorDataGrouper  , load_data
from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import numpy as np



class DataProcessor:
    def __init__(self, kafka_config, db_path, topic,time_consume_data_buffer):
        self.kafka_config = kafka_config
        self.topic = topic
        self.consumer = Consumer(self.kafka_config)
        self.consumer.subscribe([self.topic])
        

        # Shared buffer for holding data between threads
        self.data_buffer = []
        self.data_lock = threading.Lock()
 
        self.file_path = 'data_test.txt'
        self.epsilon = 2
        self.grouper = MultiSensorDataGrouper(epsilon=self.epsilon, window_size=100)
        
        self.base_moteid = 1  # Use moteid 1 as the base
        self.attribute = ['temperature','humidity','light','voltage' ] # Attribute to analyze
        
        self.time_consume_data_buffer = time_consume_data_buffer
        
        self.all_original_signals_temperature = [] 
        self.all_reconstructed_signals_temperature = [] 
        self.all_original_signals_humidity = [] 
        self.all_reconstructed_signals_humidity = [] 
        self.all_original_signals_light= [] 
        self.all_reconstructed_signals_light = [] 
        self.all_original_signals_voltage = [] 
        self.all_reconstructed_signals_voltage = [] 

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
                if current_time - last_written_time >= self.time_consume_data_buffer:
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

            if os.path.getsize(self.file_path) == 0:
                print(f"File {self.file_path} is empty. Skipping processing...")
                continue

            with self.data_lock:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                df = load_data(self.file_path)

                for attr in self.attribute:
                    # Compress and decompress data for each attribute
                    base_signal, other_signals, timestamps = self.grouper.extract_signals(df, self.base_moteid, attr)
                    # Step 2: Perform static grouping to get compression buckets
                    base_signals, ratio_signals, total_memory_cost = self.grouper.static_group(df, self.base_moteid, attr)
                    reconstructed_base_signal = self.grouper.reconstruct_signal(base_signals[0][1])

                    # Step 3: Reconstruct the compressed signals
                    # Reconstruct the base signal from its compressed buckets
                    reconstructed_other_signals = []
                    for moteid, ratio_buckets in ratio_signals.items():
                        reconstructed_signal = self.grouper.reconstruct_signal(ratio_buckets, base_signal)
                        reconstructed_other_signals.append(reconstructed_signal)

                    # Combine the base signal and other signals into one list for plotting
                    original_signals = [base_signal] + other_signals
                    reconstructed_signals = [reconstructed_base_signal] + reconstructed_other_signals

                    # Check if the list is initialized and handle concatenation
                    all_original_signals = getattr(self, f"all_original_signals_{attr}")
                    all_reconstructed_signals = getattr(self, f"all_reconstructed_signals_{attr}")

                    # Concatenate only if lists are non-empty, else assign the first values
                    if all_original_signals:
                        concatenated_original = [
                            np.concatenate((existing, new)) for existing, new in zip(all_original_signals, original_signals)
                        ]
                        setattr(self, f"all_original_signals_{attr}", concatenated_original)
                    else:
                        setattr(self, f"all_original_signals_{attr}", original_signals)

                    if all_reconstructed_signals:
                        concatenated_reconstructed = [
                            np.concatenate((existing, new)) for existing, new in zip(all_reconstructed_signals, reconstructed_signals)
                        ]
                        setattr(self, f"all_reconstructed_signals_{attr}", concatenated_reconstructed)
                    else:
                        setattr(self, f"all_reconstructed_signals_{attr}", reconstructed_signals)

                    # Debugging Prints
                    print(f"original_signals for {attr}: {original_signals}\n")
                    print(f"reconstructed_signals for {attr}: {reconstructed_signals}\n")
                    print(f'Concatenated all original_signals for {attr}: {getattr(self, f"all_original_signals_{attr}")}')
                    print(f'Concatenated all reconstructed_signals for {attr}: {getattr(self, f"all_reconstructed_signals_{attr}")}')
                    print(f'Total memory cost after compression for {attr}: {total_memory_cost} buckets')

                    # Step 4: Plot original vs reconstructed signals
                    self.grouper.plot_signals_single(original_signals, reconstructed_signals)


                self.plot_all_signals()


            with open(self.file_path, 'w'):
                pass 




    def plot_all_signals(self):
        # Loop through each attribute and plot all accumulated signals for each attribute
        for attr in self.attribute:
            # Retrieve the accumulated original and reconstructed signals for the current attribute
            all_original_signals = getattr(self, f"all_original_signals_{attr}", None)
            all_reconstructed_signals = getattr(self, f"all_reconstructed_signals_{attr}", None)

            if all_original_signals is not None and all_reconstructed_signals is not None:
                print(f"Plotting concatenated signals for {attr}...")
                # Plot the concatenated signals
                self.grouper.plot_signals_single(all_original_signals, all_reconstructed_signals)
            else:
                print(f"No concatenated signals found for {attr}")


    def start(self):
        # Create and start the consumer thread
        consumer_thread = threading.Thread(target=self.consume_messages)
        consumer_thread.start()

        # Run the compression/decompression loop in the main thread
        self.compress_decompress_and_store()

        # Wait for the consumer thread to finish (optional)
        consumer_thread.join()

    def __del__(self):
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

    processor = DataProcessor(kafka_conf, db_path, topic ,5)
    processor.start()
