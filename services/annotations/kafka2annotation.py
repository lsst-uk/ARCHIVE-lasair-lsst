import json
import sys
from confluent_kafka import Consumer
import mysql.connector as MySQLdb
import settings

def db_connector():
    """ Make a connection to the master database
    """
    try:
       conn = MySQLdb.connect (host = settings.DB_HOST,
                               user = settings.READWRITE_USER,
                             passwd = settings.READWRITE_PASS,
                                 db = 'ztf',
                               port = settings.DB_PORT)
    except MySQLdb.Error as e:
        print(("Error %d: %s" % (e.args[0], e.args[1])))
        conn = None
    return conn

def get_topics(conn):
    """ Get the list of all annotators, and form a list of topics from them
    args: 
        conn: Connection to the database
    """
    topics = []
    try:
        cursor = conn.cursor (dictionary=True)
        cursor.execute ('SELECT * from annotators where active > 0')
        for row in cursor:
            topics.append('anno_' + row['topic'])
        cursor.close ()
    except MySQLdb.Error as e:
        print("ERROR in services/annotator: cannot connect to master database, Error %d: %s\n" % (e.args[0], e.args[1]))
    return topics

def get_annotations(topics, maxannotation):
    """ Creates a Kafka consumer on the public net, logged in as admin
        args:
            topics: list of topics to consume, one per annotator
            maxannotation: max number of messages to fetch
    """
    conf = {
        'bootstrap.servers': settings.KAFKA_HOST,
        'sasl.username':     settings.KAFKA_USERNAME,
        'sasl.password':     settings.KAFKA_PASSWORD,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms':   'SCRAM-SHA-256',
        'group.id':          settings.GROUP_ID,
        'default.topic.config': {
             'auto.offset.reset': 'smallest'
        }
    }

    try:
        consumer = Consumer(conf)
    except Exception as e:
        print('ERROR cannot connect to kafka', e)
        return None
    consumer.subscribe(topics)

    annotations = []
    while len(annotations) < maxannotation:
        msg = consumer.poll(timeout=settings.KAFKA_TIMEOUT)
        if msg is None:
            break
        if msg.error():
            continue
        if msg.value() is None:
            continue
        else:
            try:
                annotation = json.loads(msg.value())
                annotations.append(annotation)
            except:
                print('ERROR not json', msg.value())
    consumer.close()
    return annotations

def process_annotations(annotations):
    """ Get all the topics of all the annotators; read all annotations on those
        then push each into the master database
    args:
        maxannotation: max number of messages to fetch
    """
    queries = []
    for annotation in annotations:
        allattrs = ['objectId', 'topic', 'version', 
            'classification', 'explanation', 'classdict', 'url']
        attrs = []
        values = []
        if not 'classification' in annotation:
            print("Classification must be non trivial string")
            continue
        for a in allattrs:
            if not a in annotation or not annotation[a]:
                continue
            v = annotation[a]
            if a == 'classdict':
                v = json.dumps(v)
            if a == 'classification' and (not v or len(v)==0):
                print("Classification must be non trivial string")
                continue
            attrs.append(a)
            values.append("'" + v + "'")
        query = 'REPLACE INTO annotations (' + ', '.join(attrs) 
        query += ') VALUES (' + ', '.join(values) + ')'
        queries.append(query)
    return queries

def execute_queries(conn, queries):
    nsuccess = 0
    cursor = conn.cursor (dictionary=True)
    for query in queries:
        try:
            cursor.execute(query)
            nsuccess += 1
        except:
            print('Query did not run: ' + query)
    conn.commit()
    return nsuccess

##################################
if __name__ == "__main__":
    # connect to master database
    conn = db_connector()

    # get all the annotation topics
    topics = get_topics(conn)
    print(len(topics), 'topics found in database')

    # how many to fetch
    try:
        maxannotation = int(sys.argv[1])
    except:
        maxannotation = 10

    # fetch from kafka
    annotations = get_annotations(topics, maxannotation)
    print(len(annotations), 'annotations found')

    # convert to queries
    queries = process_annotations(annotations)
    print(len(queries), 'queries built')

    # executr queries on master database
    nsuccess = execute_queries(conn, queries)
    print(nsuccess, ' annotations inserted to database')
