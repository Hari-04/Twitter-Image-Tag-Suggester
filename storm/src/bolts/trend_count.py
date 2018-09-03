# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
from streamparse import Bolt
from cassandra.cluster import Cluster

class TrendCountBolt(Bolt):
    #outputs = ['cls','tag','count','minute']
    
    def initialize(self, conf, ctx):
        self.counter = 0
        self.logger.info("----------------IN TREND COUNT BOLT---------------------")
        self.cluster = Cluster(['54.186.242.133','54.186.17.97']) #['54.186.242.133']
        self.session = self.cluster.connect()
        self.session.default_consistency_level=5
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS Tweet WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };")
        self.session.execute("USE Tweet;")
        self.session.execute("CREATE TABLE IF NOT EXISTS Trend(id UUID,date_entry date,hour int,class text,tag text,count int,PRIMARY KEY (id))")
        

    def process(self, tup):
        data = tup.values
        cls = str(data[0]); tag = str(data[1]); dt = str(data[2]); hour = str(data[3])
        query = "select id,count,date_entry from Trend where class='"+cls+"' and tag='"+tag+"' and hour = "+hour+" ALLOW FILTERING"
        item = self.session.execute(query)
        if item.one()!=None:
            id_from_db = item[0][0]
            count_from_db = item[0][1]
            date_from_db= item[0][2]
            if str(date_from_db) == dt:
                self.logger.info("Date Match")
                updated_count = count_from_db + 1
                query = "update Trend SET count = {} where id = {} ;".format(updated_count, id_from_db)
                self.session.execute(query)
            else:
                print("Date mismatch")
                #query = "select id from Trend where hour={} and date_entry=\'{}\' ALLOW FILTERING".format(now.hour,str(date_from_db))
                updated_count = 1;
                query = "update Trend SET count = {}, date_entry = \'{}\' where id = {} ;".format(updated_count, dt, id_from_db)
                self.session.execute(query)
                #for item in items:
                #    print("deleting: "+str(item[0]))
                #    query = "delete from Trend where id={}".format(str(item[0]))
                #    session.execute(query)
        else:
            query = "insert into Trend (id, date_entry, hour, class, tag, count) values (uuid(), \'{}\',{}, \'{}\', \'{}\',{})".format(dt, hour, cls, tag, 1)
            self.session.execute(query)
            
        #self.logger.info("----UPDATED VALUE-----{}-{}={}".format(data[0],data[1],updated_count))
        self.counter+=1
        if self.counter % 10 == 0:
            self.logger.info("Processed [{:,}] tweets".format(self.counter))
