# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

from pykafka import KafkaClient
import json
import urllib

server_ip = "54.190.16.63:9092"
local_ip = "localhost:9092"

client = KafkaClient(server_ip)
topic = client.topics['tweet']
consumer = topic.get_simple_consumer()


image_client = KafkaClient(server_ip)
image_topic = image_client.topics['image'] #b for byte stream
image_producer = image_topic.get_producer()
counter = 0

for msg in consumer:
    
    data = json.loads(msg.value)
    if 'entities' in data:
        if 'media' in data['entities']:
            if data['entities']['media'][0]['type'] == "photo" and len(data['entities']['hashtags'])>0:
                message = json.dumps({'hash':data['entities']['hashtags'],'img_url':data['entities']['media'][0]['media_url'].encode('utf-8')})
                #image_producer.produce(msg.value.encode('utf-8'))
                #print message
                image_producer.produce(message)
    
    counter+=1
    if counter%500==0:
        print "Images parsed:",counter
    