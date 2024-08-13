from pymodbus.client import ModbusTcpClient as ModbusClientTCP
from datetime import datetime, timedelta
import mysql.connector
import logging
import time

logging.basicConfig(level=logging.WARNING)

def get_meter_id(slave_id, ip):
    meter_db = mysql.connector.connect(host='192.168.1.147', user='energy', password='energypass', database="sensor_labtekvi")
    meter_cursor = meter_db.cursor()
    meter_cursor.execute("SELECT meter_id FROM meter_id WHERE slave_id = %s AND hf_ip = %s", (slave_id, ip))
    meter_id = meter_cursor.fetchone()[0]
    meter_db.close()
    return meter_id

def read_modbus_data(client, slave_id, ip):
    try:
        # Membaca data suhu dan kelembaban dengan satu kali pembacaan
        result = client.read_input_registers(1, 2, slave=slave_id)  # Membaca 2 register (1 untuk suhu dan 1 untuk kelembaban)
        
        # Mengambil data sebagai integer langsung dari register
        temperature = result.registers[0] / 10.0  # Misal, data dalam skala 10
        humidity = result.registers[1] / 10.0  # Misal, data dalam skala 10

        meter_id = get_meter_id(slave_id, ip)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:00')
        
        val = (timestamp, meter_id, temperature, humidity)
        sql = "INSERT INTO datasensor (`timestamp`, `meter_id`, `temp`, `hum`) VALUES (%s, %s, %s, %s)"
        mycursor.execute(sql, val)
        mydb.commit()

        print("IP: {}, Slave ID: {}, Meter ID: {}, Data inserted.".format(ip, slave_id, meter_id))
        print("---------------------------------------------------------------------------------------------------------------")
    
    except Exception as e:
        print("IP: {}, Slave ID: {}, error, cek koneksi atau data: {}".format(ip, slave_id, str(e)))
        print("---------------------------------------------------------------------------------------------------------------")
        time.sleep(1)

def modbus(ip, slave_ids):
    client = ModbusClientTCP(ip)
    client.connect()
    
    for slave_id in slave_ids:
        read_modbus_data(client, slave_id, ip)
    
    client.close()

# Koneksi ke database untuk menyimpan data
mydb = mysql.connector.connect(host='192.168.1.147', user='energy', password='energypass', database='sensor_labtekvi')
mycursor = mydb.cursor()

# Data Modbus IP dan Slave ID
modbus_devices = [
    {"ip": "10.6.30.182", "slave_ids": [1, 2, 3, 4]},
    {"ip": "10.6.30.178", "slave_ids": [7, 8, 9, 10, 11]}
    #{"ip": "10.6.30.xxx", "slave_ids": [5, 6, 12, 13, 14]}
]

try:
    while True:
        # Loop untuk setiap perangkat Modbus
        for device in modbus_devices:
            try:
                modbus(device["ip"], device["slave_ids"])
            except Exception as e:
                print("Error with device {}: {}".format(device['ip'],str(e)))
                continue  

        print("Waiting for the next minute...")
        time.sleep(60)  # Tunggu 60 detik sebelum mengambil data lagi

except KeyboardInterrupt:
    print("Process terminated by user.")

finally:
    mydb.close()
    print("Database connection closed.")
