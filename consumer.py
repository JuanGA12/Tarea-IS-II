from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer ("sendRow", group_id = 'id1',bootstrap_servers = ['localhost:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))
    
for message in consumer:
    person = message.value
    print ("Nombre: " + person["name"] + " \nEdad: " + person["age"]+" \n" )
