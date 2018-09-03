from itertools import cycle

from streamparse import Spout
from pykafka import KafkaClient

class DbSpout(Spout):
    outputs = ['cls','tag','date','hour']

    def initialize(self, stormconf, context):
        server_ip = "54.190.16.63:9092"
        local_ip = "localhost:9092"
        client = KafkaClient(server_ip)
        self.topic = client.topics['test4']
        self.consumer = self.topic.get_simple_consumer()
        self.it = self.consumer.__iter__()
	
    def next_tuple(self):
        
        tweet = next(self.it)
        tweet = str(tweet.value).split("--")
        #with open("tweet.txt", "w") as f:
        #    f.write(tweet.value+"\n")
        self.emit([tweet[0],tweet[1],tweet[2],tweet[3]])

    
    def ack(self, tup_id):
        pass
        #with open("ack.txt", "w+") as f:
         #   f.write(tup_id+"ERROR IN SPOUT")
    
    def fail(self, tup_id):
        pass
        #with open("error.txt", "w+") as f:
        #    f.write(tup_id+"ERROR IN SPOUT")