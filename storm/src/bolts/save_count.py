# -*- coding: utf-8 -*-
from streamparse import Bolt
from cassandra.cluster import Cluster

class SaveCountBolt(Bolt):
    #outputs = ['cls','tag','count','minute']
    
    def initialize(self, conf, ctx):
        self.counter = 0
        self.logger.info("----------------IN SAVE COUNT BOLT---------------------")
        self.cluster = Cluster(['54.186.242.133','54.186.17.97']) #['54.186.242.133']
        self.session = self.cluster.connect()
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS Tweet WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };")
        self.session.execute("USE Tweet;")
        self.session.execute("CREATE TABLE IF NOT EXISTS Popular_Tags (id UUID PRIMARY KEY, class text, tag text, count int);")
        

    def process(self, tup):
        data = tup.values
        
        query = "select id, count from Popular_Tags where class = '"+str(data[0])+"' and tag = '"+str(data[1])+"' ALLOW FILTERING"
        item = self.session.execute(query)
        if item.one()!=None:
            id_from_db = item[0][0]
            count_from_db = item[0][1]
            updated_count = count_from_db + 1
            query = "update Popular_Tags SET count = {} where id = {} ;".format(updated_count, id_from_db)
            self.session.execute(query)
        else:
            query = "insert into Popular_Tags (id, class, tag, count) values (uuid(), \'{}\', \'{}\', {})".format(data[0], data[1], 1)
            self.session.execute(query)
            updated_count = 1
            
        #self.logger.info("----UPDATED VALUE-----{}-{}={}".format(data[0],data[1],updated_count))
        self.counter+=1
        if self.counter % 10 == 0:
            self.logger.info("Processed [{:,}] tweets".format(self.counter))
