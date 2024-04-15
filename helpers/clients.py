from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.error import SerializationError
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from classes.reading import Reading
#from classes.houseplant import Houseplant
from classes.mapping import Mapping

import yaml


# Required connection configs for Confluent Cloud Schema Registry
schema_params  = {
	#"schema.registry.url": "https://psrc-4nyjd.us-central1.gcp.confluent.cloud",
	#"basic.auth.credentials.source": "USER_INFO",
	#"basic.auth.user.info": "3MR742KUFFAAJGCK:X0m13HwYaUX9HOqHK+1m3UFGPcjSeXTYsEcbzAbqHd6BlpwSvRXZaoAu18Vd9qXz"
    "schema.registry.url": "https://psrc-dq2qz.us-central1.gcp.confluent.cloud",
	"basic.auth.credentials.source": "USER_INFO",
	"basic.auth.user.info": "JPOXAOSRY2M5QCSP:pEZfn81JvE2sx6QgVvQ3v0IfnvGTgZqCrzCxgJpc3I/SkqIPLbsVgzHPycPmg0/D"
}

# Configuración del cliente del Registro de Esquemas
schema_registry_conf = {
	"url": schema_params["schema.registry.url"],
	#"basic.auth.credentials.source": schema_params["basic.auth.credentials.source"],
	"basic.auth.user.info": schema_params["basic.auth.user.info"]
}


def config():
	# fetches the configs from the available file
	with open('./config/config2.yaml', 'r') as config_file:
		config = yaml.load(config_file, Loader=yaml.CLoader)
		#config = yaml.safe_load(config_file)
		return config


def sr_client():
	# set up schema registry
#	sr_conf = config()['schema-registry']
	sr_client = SchemaRegistryClient(schema_registry_conf)

	return sr_client

def reading_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Reading.get_schema(),
		from_dict = Reading.dict_to_reading
		)

'''
def houseplant_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Houseplant.get_schema(),
		from_dict = Houseplant.dict_to_houseplant
		)
'''

def mapping_deserializer():
	return AvroDeserializer(
		schema_registry_client = sr_client(),
		schema_str = Mapping.get_schema(),
		from_dict = Mapping.dict_to_mapping
		)


def reading_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = Reading.get_schema(),
		to_dict = Reading.reading_to_dict
		)

'''
def houseplant_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = Houseplant.get_schema(),
		to_dict = Houseplant.houseplant_to_dict
		)
'''

def mapping_serializer():
	return AvroSerializer(
		schema_registry_client = sr_client(),
		schema_str = Mapping.get_schema(),
		to_dict = Mapping.mapping_to_dict
		)


def producer(value_serializer):
	producer_conf = config()['kafka'] | { 'value.serializer': value_serializer }
	return SerializingProducer(producer_conf)
    
def consumer(value_deserializer, group_id, topics):
    consumer_conf = config()['kafka'] | {'value.deserializer': value_deserializer,
										  'group.id': group_id,
										  'auto.offset.reset': 'earliest',
										  'enable.auto.commit': 'false'
										  }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(topics)
    return consumer