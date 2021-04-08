from cassandra.cluster import Cluster
import hashlib
CASSANDRA_HOST  = ['192.168.0.17', '192.168.0.11', '192.168.0.18', '192.168.0.39', '192.168.0.23']

class cassandra():
    def __init__(self):
        from cassandra.cluster import Cluster
        from cassandra.query import dict_factory
        keyspace = 'lasair'
        self.table = 'candidates'
        self.cluster = Cluster(CASSANDRA_HOST)
        self.session = self.cluster.connect()
        self.session.row_factory = dict_factory
        self.session.set_keyspace(keyspace)

    def lightcurve(self, objectId):
        query = """select * from candidates where objectId = %s"""
        candlist = self.session.execute(query, (objectId,))
        candidates = []

        for c in candlist:
            candidates.append(c)

        return {'objectId':objectId, 'candidates':candidates}

    def shutdown(self):
        self.cluster.shutdown()
