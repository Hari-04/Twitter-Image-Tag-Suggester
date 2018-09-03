# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster

import datetime

cluster = Cluster(['54.186.242.133','54.186.17.97'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS kong WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };")
session.execute("USE kong;")
session.execute("CREATE TABLE IF NOT EXISTS Tagcount (id UUID PRIMARY KEY, class text, tag text, count int);")

session.execute("CREATE TABLE IF NOT EXISTS Trend(id UUID,date_entry date,hour int,class text,tag text,count int,PRIMARY KEY (id))")

clas = 'mountain'
tag = 'Flatiron'
count = 2
date= "2018-05-07"
hr = 14

#query = "insert into Trend (id, date_entry, hour, class, tag, count) values (uuid(), \'{}\',{}, \'{}\', \'{}\',{})".format(date, hr, clas, tag, count)
#session.execute(query)

# query = "select id, count from Tagcount where class = 'mountain' and tag = 'boulder' ALLOW FILTERING"
# item = session.execute(query)
# if len(id_from_db)>0:
#     id_from_db = item[0][0]
#     count_from_db = item[0][1]
#     updated_count = count_from_db + 1
# else:
#     updated_count = 1

# #Reteival
# query = "select id,tag,max(count) from Tagcount where class = 'mountain' ALLOW FILTERING"
# item = session.execute(query)
# id_from_db = item[0][0]
# tag_from_db = item[0][1]
# count_from_db = item[0][2]
# print("[Retreival ] {} :  {} => {} ".format(id_from_db,tag_from_db,count_from_db))


#Trend TABLE
now = datetime.datetime.now()
now_date = "{:04}-{:02}-{:02}".format(now.year,now.month,now.day)
#print now

#
query = "select id,count,date_entry from Trend where class='mountain' and tag='Flatiron' and hour = 14 ALLOW FILTERING"
item = session.execute(query)
if item.one()!=None:
    id_from_db = item[0][0]
    count_from_db = item[0][1]
    date_from_db= item[0][2]
    #now_date = "2018-05-07"
    if str(date_from_db) == now_date:
        print("Date Match")
        updated_count = count_from_db + 1
        query = "update Trend SET count = {} where id = {} ;".format(updated_count, id_from_db)
        session.execute(query)
    else:
        print("Date mismatch")
        #query = "select id from Trend where hour={} and date_entry=\'{}\' ALLOW FILTERING".format(now.hour,str(date_from_db))
        updated_count = 1;
        query = "update Trend SET count = {}, date_entry = \'{}\' where id = {} ;".format(updated_count, now_date,id_from_db)
        session.execute(query)
        #for item in items:
        #    print("deleting: "+str(item[0]))
        #    query = "delete from Trend where id={}".format(str(item[0]))
        #    session.execute(query)
else:
    query = "insert into Trend (id, date_entry, hour, class, tag, count) values (uuid(), \'{}\',{}, \'{}\', \'{}\',{})".format(now_date, now.hour, clas, tag, 1)
    session.execute(query)
    
# print("Date from db",str(date_from_db))

# else:
#     updated_count = 1
# query = "insert into Trend (id,date_entry,hour, class, tag, count) values (uuid(), \'{}\', {},\'{}\',\'{}\', {})".format(now_date,now.hour,clas, tag, 1)
# session.execute(query)

#query = "select id,date_entry,hour,class,tag,count from Trend"
#item = session.execute(query)
#print("[RET] {} ".format(item[0]))

# query = "update Tagcount SET count = {} where id = {} ;".format(updated_count, id_from_db)
# print(query)
# session.execute(query)

#session.execute("""insert into Tagcount (id, class, tag) values (uuid(), 'mountain', 'Flatirons')""")

#result = session.execute("select * from user where lastname='Jones'")[0]
# result = session.execute("select * from user")
# for item in result:
#     print(item.lastname, item.age)