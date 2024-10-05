import sqlite3
import time
import zlib
import json


DB_FILE = 'data_comparison.db'



def monitor_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    while True:
        print("\n--- Monitoring Database ---")
        
        
        cursor.execute('''
            SELECT id, timestamp, original_data, compressed_data, decompressed_data 
            FROM data_comparison
            ORDER BY id DESC
        ''')
        rows = cursor.fetchall()

        for row in rows:
            id, timestamp, original_data, compressed_data, decompressed_data = row
            
            # Try decompressing the compressed data to display it
            compressed_data_size = len(compressed_data)
            decompressed_data_size = len(decompressed_data.encode('utf-8'))
            original_data_size = len(original_data.encode('utf-8'))

            print(f"\nID: {id}")
            print(f"Timestamp: {timestamp}")
            print(f"Original Data: {original_data}")
            print(f"Original Data Size: {original_data_size} bytes")
            print(f"Compressed Data Size: {compressed_data_size} bytes")
            print(f"Decompressed Data: {decompressed_data}")
            print(f"Decompressed Data Size: {decompressed_data_size} bytes")

        print("\n--- End of Monitoring ---")
        
        # Sleep for a while before querying again (adjust the time as needed)
        time.sleep(5)

if __name__ == "__main__":
    monitor_db()
