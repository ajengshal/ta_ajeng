import time
from datetime import datetime, timedelta
from pymodbus.client import ModbusTcpClient
import mysql.connector
from mysql.connector import Error

# Modbus connection details
MODBUS_IP = '10.6.30.182'
MODBUS_PORT = 502

# Device details
DEVICES = [
    {'address': 1, 'count': 2, 'slave_id': 1},
    {'address': 1, 'count': 2, 'slave_id': 2},
    {'address': 1, 'count': 2, 'slave_id': 3},
    {'address': 1, 'count': 2, 'slave_id': 4}
]

# MySQL connection details
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = ''
MYSQL_DATABASE = 'test'

# Retry settings
RETRY_COUNT = 3  # Number of retries for each device
RETRY_DELAY = 1  # Delay between retries in seconds

def read_modbus_data(client, device):
    for attempt in range(RETRY_COUNT):
        try:
            response = client.read_input_registers(device['address'], device['count'], slave=device['slave_id'])
            if not response.isError():
                return response.registers
            else:
                print(f"Error reading Modbus data for device {device['slave_id']}: {response}")
        except Exception as e:
            print(f"Exception reading Modbus data for device {device['slave_id']}, attempt {attempt+1}: {e}")
        time.sleep(RETRY_DELAY)
    return [0, 0]  # Return [0, 0] after all retries fail

def insert_data_to_mysql(cursor, all_data):
    try:
        query = """
        INSERT INTO hf_gallery (timestamp, temp1, hum1, temp2, hum2, temp3, hum3, temp4, hum4)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(query, [timestamp] + all_data)
    except Error as e:
        print(f"Error inserting data into MySQL: {e}")

def main():
    # Set up Modbus client
    client = ModbusTcpClient(MODBUS_IP, port=MODBUS_PORT, timeout=10)  # Increased timeout to 10 seconds
    if not client.connect():
        print("Failed to connect to Modbus server")
        return

    # Set up MySQL connection
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
        else:
            print("Failed to connect to MySQL database")
            return
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return

    try:
        while True:
            # Get the current time and calculate the next time to insert data
            now = datetime.now()
            next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)

            # Sleep until the beginning of the next minute
            time_to_sleep = (next_minute - now).total_seconds()
            time.sleep(time_to_sleep)

            # Collect data from all devices
            all_data = []
            for device_id, device in enumerate(DEVICES, start=1):
                data = read_modbus_data(client, device)
                all_data.extend(data)  # Add the readings (or [0, 0] in case of error) to the all_data list
                time.sleep(0.5)  # Adding delay between device reads to avoid overwhelming devices

            # Insert data into MySQL database
            insert_data_to_mysql(cursor, all_data)
            connection.commit()

    except KeyboardInterrupt:
        print("Process interrupted")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
        client.close()

if __name__ == "__main__":
    main()
