import time
import logging
import yaml

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

#import avro_helper
#importo classes
from classes.houseplant import Houseplant
from helpers import clients


# Required connection configs for Confluent Cloud Schema Registry
schema_params  = {
    "schema.registry.url": "https://psrc-dq2qz.us-central1.gcp.confluent.cloud",
	"basic.auth.credentials.source": "USER_INFO",
	"basic.auth.user.info": "JPOXAOSRY2M5QCSP:pEZfn81JvE2sx6QgVvQ3v0IfnvGTgZqCrzCxgJpc3I/SkqIPLbsVgzHPycPmg0/D"
}

# Configuración del cliente del Registro de Esquemas
schema_registry_conf = {
	"url": schema_params["schema.registry.url"],
	"basic.auth.user.info": schema_params["basic.auth.user.info"]
}

def config():
	# fetches the configs from the available file
	with open('./config/config2.yaml', 'r') as config_file:
		config = yaml.load(config_file, Loader=yaml.CLoader)
		#config = yaml.safe_load(config_file)
		return config
        
conf = config()

# set up schema registry
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

houseplant_avro_serializer = AvroSerializer(
        schema_registry_client = schema_registry_client,
        schema_str = Houseplant.get_schema(),
        to_dict = Houseplant.houseplant_to_dict
)

# set up Kafka producer
producer = clients.producer(houseplant_avro_serializer)

topic = 'houseplant_metadata'

# existing plants
plants = {
    '2': {
        "plant_id": 2,
        "scientific_name": "Succulentus",
        "common_name": "Suculentas",
        "given_name": "Bici",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 20,
        "moisture_high": 35
    },
    '1': {
        "plant_id": 1,
        "scientific_name": "Spathiphyllum",
        "common_name": "Cuna de Moisés",
        "given_name": "Paz",
        "temperature_low": 15,
        "temperature_high": 30,
        "moisture_low": 60,
        "moisture_high": 90
    }
}

for k,v in plants.items():
    # send data to Kafka
    houseplant = Houseplant(
            v["plant_id"], 
            v["scientific_name"],
            v["common_name"],
            v["given_name"],
            v["temperature_low"],
            v["temperature_high"],
            v["moisture_low"],
            v["moisture_high"]
        )
    producer.produce(topic, key=str(k), value=houseplant) 
    producer.poll()
