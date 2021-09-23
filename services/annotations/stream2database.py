import json
from confluent_kafka import Consumer
import mysql.connector as MySQLdb
import settings

def make_consumer(group_id, topics):
    """ Creates a Kafka consumer on the public net, logged in as admin
        args:
            group_id: group_id (string) -- where to start the consuming
            topics: list of topics to consume, one per annotator
    """
    conf = {
        'bootstrap.servers': settings.KAFKA_HOST,
        'sasl.username':     settings.KAFKA_USERNAME,
        'sasl.password':     settings.KAFKA_PASSWORD,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms':   'SCRAM-SHA-256',
        'group.id':          group_id,
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
    return consumer

def get_messages(consumer, maxalert):
    """ Gets some messages from the Kafka topics
    args:
        maxalert: max number of messages to fetch
    """
    alerts = []
    while len(alerts) < maxalert:
        msg = consumer.poll(timeout=settings.KAFKA_TIMEOUT)
        if msg is None:
            break
        if msg.error():
            continue
        if msg.value() is None:
            continue
        else:
            try:
                alert = json.loads(msg.value())
                alerts.append(alert)
            except:
                print('ERROR not json', msg.value())
    return alerts

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
        cursor.execute ('SELECT * from annotators')
        for row in cursor:
            topics.append('anno_' + row['topic'])
        cursor.close ()
    except MySQLdb.Error as e:
        print("ERROR in services/TNS: cannot connect to master database, Error %d: %s\n" % (e.args[0], e.args[1]))
    return topics

def process_annotations(maxalert):
    """ Get all the topics of all the annotators; read all annotations on those
        then push each into the master database
    args:
        maxalert: max number of messages to fetch
    """
    conn = db_connector()
    if not conn:
        print("ERROR in services/annotator: Cannot connect to the database\n")
    topics = get_topics(conn)
    print('Using topics', topics)

    consumer = make_consumer(settings.GROUP_ID, topics)

    alerts = get_messages(consumer, maxalert)
    
    cursor = conn.cursor (dictionary=True)
    nquery = 0
    for alert in alerts:
        allattrs = ['objectId', 'topic', 'version', 
            'classification', 'explanation', 'classdict', 'url']
        attrs = []
        values = []
        for a in allattrs:
            if not a in alert or not alert[a]:
                continue
            v = alert[a]
            if a == 'classdict':
                v = json.dumps(v)
            if a == 'classification' and (not v or len(v)==0):
                print("Classification must be non trivial string")

                continue
            attrs.append(a)
            values.append("'" + v + "'")
        query = 'REPLACE INTO annotations (' + ', '.join(attrs) 
        query += ') VALUES (' + ', '.join(values) + ')'
        cursor.execute(query)
        nquery += 1
    consumer.close()
    return nquery

if __name__ == "__main__":
    nquery = process_annotations(10)
    print(nquery, ' annotations inserted')
