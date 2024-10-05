import json
import matplotlib.pyplot as plt
from confluent_kafka import Consumer, KafkaException, KafkaError
import threading

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:19092',  # Broker list
    'group.id': 'my_consumer_group',  # Consumer group
    'auto.offset.reset': 'earliest',  # Start from the earliest message
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'superuser',
    'sasl.password': 'secretpassword'
}

# Create a Kafka Consumer instance
consumer = Consumer(consumer_conf)
consumer.subscribe(['sensor_data'])

# Initialize lists to store incoming data
timestamps = []
temperatures = []
humidities = []
lights = []
voltages = []

# Lock for thread synchronization
data_lock = threading.Lock()

# Set up the plotting
plt.ion()  # Enable interactive mode
fig, ax = plt.subplots(4, 1, figsize=(12, 16))  # Increased figure size for better visibility

# Function to update the graph dynamically
def update_graph():
    while True:
        with data_lock:
            ax[0].clear()
            ax[1].clear()
            ax[2].clear()
            ax[3].clear()
            
            # Temperature Plot
            ax[0].plot(timestamps, temperatures, 'r-', label='Temperature (°C)')
            ax[0].set_title('Real-time Temperature Data')
            ax[0].set_xlabel('Timestamp')
            ax[0].set_ylabel('Temperature (°C)')
            ax[0].set_ylim(-10, 50)  # Adjust y-axis scale for temperature
            ax[0].legend()
            ax[0].tick_params(axis='x', rotation=45)

            # Humidity Plot
            ax[1].plot(timestamps, humidities, 'b-', label='Humidity (%)')
            ax[1].set_title('Real-time Humidity Data')
            ax[1].set_xlabel('Timestamp')
            ax[1].set_ylabel('Humidity (%)')
            ax[1].set_ylim(0, 100)  # Adjust y-axis scale for humidity
            ax[1].legend()
            ax[1].tick_params(axis='x', rotation=45)
            
            # Light Plot
            ax[2].plot(timestamps, lights, 'g-', label='Light (lux)')
            ax[2].set_title('Real-time Light Data')
            ax[2].set_xlabel('Timestamp')
            ax[2].set_ylabel('Light (lux)')
            ax[2].set_ylim(0, 1000)  # Adjust y-axis scale for light
            ax[2].legend()
            ax[2].tick_params(axis='x', rotation=45)
            
            # Voltage Plot
            ax[3].plot(timestamps, voltages, 'm-', label='Voltage (V)')
            ax[3].set_title('Real-time Voltage Data')
            ax[3].set_xlabel('Timestamp')
            ax[3].set_ylabel('Voltage (V)')
            ax[3].set_ylim(0, 5)  # Adjust y-axis scale for voltage
            ax[3].legend()
            ax[3].tick_params(axis='x', rotation=45)

        plt.tight_layout()
        plt.draw()
        plt.pause(0.1)  # Pause for a short time to allow updates

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
                # Successfully received a message
                try:
                    # Parse the message as JSON
                    message_value = msg.value().decode('utf-8')
                    data = json.loads(message_value)
                    
                    # Extract data fields
                    timestamp = f"{data['date']} {data['time']}"
                    temperature = data['temperature']
                    humidity = data['humidity']
                    light = data['light']
                    voltage = data['voltage']

                    # Use lock to safely update shared data
                    with data_lock:
                        timestamps.append(timestamp)
                        temperatures.append(temperature)
                        humidities.append(humidity)
                        lights.append(light)
                        voltages.append(voltage)

                except json.JSONDecodeError:
                    print(f"Received non-JSON message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass  # Clean exit on Ctrl+C
    finally:
        # Close the consumer
        consumer.close()

# Main function to start the consumer and plot the data
if __name__ == "__main__":
    # Create and start the consumer thread
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.start()

    # Start graph update in the main thread
    update_graph()

    # Wait for consumer thread to finish (optional)
    consumer_thread.join()
