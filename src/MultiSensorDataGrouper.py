import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import pandas as pd


class MultiSensorDataGrouper:
    def __init__(self, epsilon, window_size=100):
        self.epsilon = epsilon
        self.window_size = window_size
        self.index_structure = {}  # Dictionary to store the index structure

    # Bucket creation for a signal
    def create_buckets(self, signal, epsilon):
        """
        Create buckets for a given signal based on the epsilon threshold.
        
        :param signal: Array or list of signal values.
        :param epsilon: Tolerance for bucketing.
        :return: List of buckets, each containing the average value and original values.
        """
        buckets = []
        current_bucket = []
        min_value, max_value = float('inf'), float('-inf')
        
        for value in signal:
            current_bucket.append(value)
            min_value = min(min_value, value)
            max_value = max(max_value, value)

            if max_value - min_value > 2 * epsilon:
                average = (min_value + max_value) / 2
                buckets.append((average, current_bucket))  # Store average and len
                current_bucket = []
                min_value, max_value = float('inf'), float('-inf')

        if current_bucket:
            average = (min_value + max_value) / 2
            buckets.append((average, current_bucket))
        
        return buckets

    def static_group(self, df, base_moteid, attribute):
        """
        Perform static grouping on the DataFrame using a specified base moteid and a single attribute.

        :param df: DataFrame containing sensor data with 'timestamp', 'moteid', and various attributes.
        :param base_moteid: The moteid to use as the base for grouping.
        :param attribute: The attribute (column) to group and calculate ratios for.
        :return: Tuple of base signals, ratio signals, and total memory cost.
        """
        base_signals = []
        ratio_signals = {}

        # Get the base signal for the specified moteid
        base_signal_df = df[df['moteid'] == base_moteid].set_index('timestamp')[attribute].reset_index()

        # Iterate over unique moteids except the base moteid
        for moteid in df['moteid'].unique():
            if moteid == base_moteid:
                continue

            # Get the signal for the current moteid
            signal_df = df[df['moteid'] == moteid].set_index('timestamp')[attribute].reset_index()

            # Merge the base and signal DataFrames on timestamp
            merged_df = base_signal_df.merge(signal_df, on='timestamp', suffixes=('_base', '_signal'), how='inner')

            # Filter out rows with NaN values in either column
            merged_df = merged_df.dropna()

            # Calculate the ratio if both base and signal exist
            if f'{attribute}_base' in merged_df and f'{attribute}_signal' in merged_df:
                # Calculate the ratio
                merged_df[f'ratio_{attribute}'] = merged_df[f'{attribute}_signal'] / merged_df[f'{attribute}_base']

                # Create buckets for the calculated ratio
                ratio_buckets = self.create_buckets(merged_df[f'ratio_{attribute}'], 0.1)
                ratio_signals[moteid] = ratio_buckets

        # Create buckets for the base signal
        base_buckets = self.create_buckets(base_signal_df[attribute], self.epsilon)
        base_signals.append((base_moteid, base_buckets))

        total_memory = self.calculate_memory_cost(base_signals, ratio_signals)
        return base_signals, ratio_signals, total_memory

    # Calculate the memory cost of compression (number of buckets)
    def calculate_memory_cost(self, base_signals, ratio_signals):
        """
        Calculate the memory cost based on the number of buckets.

        :param base_signals: Base signals with their buckets.
        :param ratio_signals: Ratio signals with their buckets.
        :return: Total memory cost (number of buckets).
        """
        memory_cost = 0

        # Memory cost for base signals
        for base_buckets in base_signals:
            memory_cost += len(base_buckets[1])  # len of bucket values

        # Memory cost for ratio signals
        for ratio_buckets in ratio_signals.values():
            memory_cost += len(ratio_buckets)

        return memory_cost

    # Function to reconstruct signal from buckets
    def reconstruct_signal(self, buckets, base_signal=None):
        """
        Reconstruct the signal from its buckets by replacing each value with the average of the bucket.

        :param buckets: List of buckets (average, original values).
        :param base_signal: Optional base signal for reconstructing based on base.
        :return: Reconstructed signal.
        """
        reconstructed_signal = []

        for average, values in buckets:
            if base_signal is not None:
                # Reconstruct based on the base signal and average
                base_value = base_signal[len(reconstructed_signal)]  # Use modulo for indexing
#                 print(base_value, average, values)
                reconstructed_signal.extend([base_value * average] * len(values))  # Multiply by the average ratio
            else:
                reconstructed_signal.extend([average] * len(values))  # Use average only for standard reconstruction
        
        return reconstructed_signal

    def plot_signals(self, original_signals, compressed_buckets):
        """
        Plot the original and compressed signals in a single plot for comparison.

        :param original_signals: List of original signals.
        :param compressed_buckets: List of compressed signals (buckets).
        """
        num_signals = len(original_signals)

        # Create a single figure for all signals
        plt.figure(figsize=(10, 6))

        for i, (original, buckets) in enumerate(zip(original_signals, compressed_buckets)):
            compressed_signal = self.reconstruct_signal(buckets)

            # Plot original signal
            plt.plot(original, linestyle='-', color=f'C{i}')

            # Plot compressed signal
            plt.plot(compressed_signal, linestyle='--', color=f'C{i}', alpha=0.8)

        plt.title('Original and Compressed Signals')
        plt.xlabel('Time')
        plt.ylabel('Signal Value')
        plt.legend(loc='best')  # Automatically adjusts to best position
        plt.tight_layout()
        plt.show()

    def extract_signals(self, df, base_moteid, attribute):
        """
        Extract the base signal and other related signals for plotting.

        :param df: DataFrame containing sensor data with 'timestamp', 'moteid', and various attributes.
        :param base_moteid: The moteid to use as the base for extracting signals.
        :param attribute: The attribute (column) to extract signals for (e.g., temperature, humidity).
        :return: Tuple of base signal, list of other signals, and timestamps.
        """
        # Extract base signal
        base_signal_df = df[df['moteid'] == base_moteid].set_index('timestamp')[attribute].reset_index()

        # Extract other signals (all moteids except the base)
        other_signals = []
        timestamps = base_signal_df['timestamp'].values

        for moteid in df['moteid'].unique():
            if moteid == base_moteid:
                continue

            # Extract signal for the current moteid
            signal_df = df[df['moteid'] == moteid].set_index('timestamp')[attribute].reset_index()

            # Merge base signal and current signal on timestamp to align them
            merged_df = base_signal_df.merge(signal_df, on='timestamp', suffixes=('_base', '_signal'), how='inner')

            # If timestamps align, extract the signal for plotting
            if not merged_df.empty:
                other_signals.append(merged_df[f'{attribute}_signal'].values)

        # Return the base signal and list of other signals
        base_signal = base_signal_df[attribute].values
        return base_signal, other_signals, timestamps

    def plot_signals_single(self, original_signals, reconstructed_signals):
        """
        Plot original vs reconstructed signals for each signal in separate subplots.

        :param original_signals: List of original signals.
        :param reconstructed_signals: List of reconstructed signals.
        """
        num_signals = len(original_signals)

        fig, axes = plt.subplots(num_signals, 1, figsize=(10, 5 * num_signals))
        if num_signals == 1:
            axes = [axes]  # Handle single plot case

        for i, (original, reconstructed) in enumerate(zip(original_signals, reconstructed_signals)):
            # Plot original vs reconstructed
            axes[i].plot(original, label='Original Signal', color='blue')
            axes[i].plot(reconstructed, label='Reconstructed Signal', color='orange', linestyle='--')
            axes[i].set_title(f'Signal {i + 1}')
            axes[i].legend()

        plt.tight_layout()
        plt.show()
        
        
        
def load_data(file_path):
    # Load the data from a text file without parsing dates upfront
    df = pd.read_csv(file_path, sep=' ', header=None,
                     names=['date', 'time', 'epoch', 'moteid', 'temperature', 'humidity', 'light', 'voltage'])
    
    # Combine 'date' and 'time' columns into a single datetime column
    df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
    
    # Drop rows where 'date_time' couldn't be parsed correctly
    df.dropna(subset=['date_time'], inplace=True)
    
    # Drop the original 'date' and 'time' columns
    df.drop(columns=['date', 'time'], inplace=True)
    
    # Sort by 'date_time'
    df.sort_values(by='date_time', inplace=True)
    
    # Drop rows where other columns contain NaN
    df.dropna(subset=['moteid', 'temperature', 'humidity', 'voltage', 'light'], inplace=True)
    df['moteid'] = df['moteid'].astype(int)
    df['timestamp'] = df['date_time'].dt.floor('30s')
    return df


if __name__ == "__main__":
    file_path = 'data.txt'
    epsilon = 2.0
    grouper = MultiSensorDataGrouper(epsilon=epsilon, window_size=100)
    base_moteid = 1  # Use moteid 1 as the base
    attribute = 'temperature'  # Attribute to analyze


    df = load_data(file_path)
    print(df)
    
    base_signal, other_signals, timestamps = grouper.extract_signals(df, base_moteid, attribute)

    # Step 2: Perform static grouping to get compression buckets
    base_signals, ratio_signals, total_memory_cost = grouper.static_group(df, base_moteid, attribute)

    # Step 3: Reconstruct the compressed signals
    # Reconstruct the base signal from its compressed buckets
    reconstructed_base_signal = grouper.reconstruct_signal(base_signals[0][1])

    # Reconstruct the other signals from their ratio buckets
    reconstructed_other_signals = []
    for moteid, ratio_buckets in ratio_signals.items():
        reconstructed_signal = grouper.reconstruct_signal(ratio_buckets, base_signal)
        reconstructed_other_signals.append(reconstructed_signal)

    # Step 4: Plot original vs reconstructed signals

    # Combine the base signal and other signals into one list for plotting
    original_signals = [base_signal] + other_signals
    reconstructed_signals = [reconstructed_base_signal] + reconstructed_other_signals

    # Output the total memory cost
    print(f'Total memory cost after compression: {total_memory_cost} buckets')