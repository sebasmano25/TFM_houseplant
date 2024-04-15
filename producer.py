#!/usr/bin/env python

import time
from board import SCL, SDA
import busio
from adafruit_seesaw.seesaw import Seesaw
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer


#importo classes
from classes.reading import Reading

#importo funciones

from helpers import clients
config = clients.config()

# Configuración de los sensores
sensor_addresses = {
    0x36: "1",
    0x37: "2"
}



def produce_sensor_readings(producer):
    # Aquí debes incluir la lógica para obtener la lectura del sensor de humedad y temperatura
    
    try:
        i2c_bus = busio.I2C(SCL, SDA)
        for address, sensor_name in sensor_addresses.items():
            ss = Seesaw(i2c_bus, addr=address)
            
            # Defino los llimites de humedad aceptados
            sensor_low = 400
            sensor_high = 1200

            # Leer humedad
            touch = ss.moisture_read()
            if touch < sensor_low:
                touch = sensor_low
            elif touch > sensor_high:
                touch = sensor_high

            touch_percent = (touch - sensor_low) / (sensor_high - sensor_low) * 100

            # Leer temperatura
            temp = ss.get_temp()

            # Crear objeto Reading con los datos del sensor
            reading = Reading(int(sensor_name), round(touch_percent, 3), round(temp, 3))

            producer.produce(config['topics']['readings'], key=sensor_name.encode('utf-8'), value=reading)
            
            print(f"Reading from {sensor_name} sent to Kafka")
            print(reading.to_dict())

            
    except Exception as e:
        print("Got exception %s", e)
    finally:
        producer.poll()
        producer.flush()


def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


if __name__ == '__main__':


    # set up Kafka Producer for Readings
    producer = clients.producer(clients.reading_serializer())

    try:
        while True:


            produce_sensor_readings(producer)

            # Esperar un tiempo antes de la próxima lectura y envío
            time.sleep(5)

    finally:
        # Flushear y cerrar el productor Kafka al salir
        producer.flush()
        #producer.close()
