"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.image_classify import TagCountBolt
from spouts.db_stream import DbSpout
from bolts.save_count import SaveCountBolt
from bolts.trend_count import TrendCountBolt

class DBStorm(Topology):
    db_spout = DbSpout.spec()
    #count_bolt = TagCountBolt.spec(inputs=[db_spout],par=2)
    cassandra_bolt = SaveCountBolt.spec(inputs={db_spout: Grouping.fields(['cls','tag'])},par=2) #SaveCountBolt.spec(inputs=[count_bolt],par=2)
    cassandra_trend_bolt = TrendCountBolt.spec(inputs={db_spout: Grouping.fields(['cls','tag'])},par=2)