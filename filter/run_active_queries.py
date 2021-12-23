""" Runs the user's queries. 
Fetch them from database, construct SQL, execute, produce kafka

(1) fetch_queries(): 
gets all the queries from the database

(2) run_annotation_queries(query_list): 
may be called to get all the recent fast annotations 

(3) run_queries(query_list, annotation_list=None):
Uses query_list and possibly annotation_list and runs all the queries against 
local, or those involving annotator against master databaase

(4) query_for_object(query, objectId):
If doing fast annotations, convert a given query with specific objectId

(5) run_query(query, msl, annotator=None, objectId=None):
Run a specific query and return query_results

(6) dispose_query_results(query, query_results):
Deal with the query results

(6a) fetch_digest(topic_name):
    Get the digest file from shared storage

(6b) dispose_email(allrecords, last_email):
    Deal with outgoing emails, it calls this to actually send
    send_email(email, topic, message, message_html):

(6c) dispose_kafka(query_results, topic):
    Produce Kafka output to public stream

"""

import os, sys
import time
import json
import settings
from confluent_kafka import Consumer, Producer, KafkaError
import datetime
import mysql.connector
import smtplib
#from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def fetch_queries():
    """fetch_queries.
    Get all the stored queries from the master database
    """
    # first get the user queries from the database that the webserver uses
    msl_remote = db_connect_remote()

    # Fetch all the stored queries from the master database
    cursor   = msl_remote.cursor(buffered=True, dictionary=True)
    query = 'SELECT mq_id, user, name, email, tables, active, real_sql, topic_name '
    query += 'FROM myqueries, auth_user WHERE myqueries.user = auth_user.id AND active > 0'
    cursor.execute(query)

    query_list = []
    for query in cursor:
        query_dict = {
            'mq_id':     query['mq_id'],
            'user':      query['user'],
            'name':      query['name'],
            'active':    query['active'],
            'email':     query['email'],
            'tables':    query['tables'],
            'real_sql':  query['real_sql'],
            'topic_name':query['topic_name'],
        }
        query_list.append(query_dict)
    return query_list

def run_annotation_queries(query_list):
    """run_annotation_queries.
    Pulls the recent content from the kafka topic 'ztf_annotations' 
    Each message has an annotator/topic name, and the objectId that was annotated.
    Queries that have that annotator should run against that object
    """
    annotation_list = []
    conf = {
        'bootstrap.servers':   settings.KAFKA_HOST,
        'group.id':            settings.ANNOTATION_GROUP_ID,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }
    streamReader = Consumer(conf)
    topic = 'ztf_annotations'
    streamReader.subscribe([topic])
    while 1:
        msg = streamReader.poll(timeout=5)
        if msg == None: break
        ann = json.loads(msg.value())
        annotation_list.append(ann)
    streamReader.close()
    #print('got ', annotation_list)
    run_queries(query_list, annotation_list)

def run_queries(query_list, annotation_list=None):
    """
    When annotation_list is None, it runs all the queries against the local database
    When not None, runs some queires agains a specific object, using the master database
    """
    try:
        msl_local = db_connect_local()
    except:
        print('ERROR in filter/run_active_queries: cannot connect to local database')
        sys.stdout.flush()

    for query in query_list:
        n = 0
        t = time.time()

        # normal case of streaming queries
        if not annotation_list:  
            query_results = run_query(query, msl_local)
            n += dispose_query_results(query, query_results)

        # immediate response to active=2 annotators
        else:
            for ann in annotation_list:  
                msl_remote = db_connect_remote()
                query_results = run_query(query, msl_remote, ann['annotator'], ann['objectId'])
                n += dispose_query_results(query, query_results)

        t = time.time() - t
        if n > 0:
            print('   %s got %d in %.1f seconds' % (query['topic_name'], n, t))
            sys.stdout.flush()

def query_for_object(query, objectId):
    """ modifies an existing query to add a new constraint for a specific object.
    We already know this query comes from multiple tables: objects and annotators,
    so we know there is an existing WHERE clause. Can add the new constraint to the end,
    unless there is an ORDER BY, in which case it comes before that.

    Args:
        query: the original query, as generated from the Lasair query builder
        objectId: the object that is the new constraint
    """
    tok = query.replace('order by', 'ORDER BY').split('ORDER BY')
    query = tok[0] + (' AND objects.objectId="%s" ' % objectId)
    if len(tok) == 2: # has order clause, add it back
        query += ' ORDER BY ' + tok[1]
    return query

def run_query(query, msl, annotator=None, objectId=None):
    """run_query. Two cases here: 
    if annotator=None, runs the query against the local database
    if annotator and objectId, checks if the query involves the annotator, 
        and if so, runs the query for the given object on master database

    Args:
        query:
        msl:
        annotation_list:
    """
    active = query['active']
    email = query['email']
    topic = query['topic_name']
    limit = 1000

    sqlquery_real = query['real_sql']
    if annotator:
        # if the annotator does not appear in the query tables, then we don't need to run it
        if not annotator in query['tables']:
            return []
        # run the query against master for this specific object that has been annotated
        sqlquery_real = query_for_object(sqlquery_real, objectId)

    # in any case, limit the output
    sqlquery_real += (' LIMIT %d' % limit)

    cursor = msl.cursor(buffered=True, dictionary=True)
    n = 0
    query_results = []
    try:
        cursor.execute(sqlquery_real)
        for record in cursor:
            recorddict = dict(record)
            utcnow = datetime.datetime.utcnow()
            recorddict['UTC'] = utcnow.strftime("%Y-%m-%d %H:%M:%S")
            #print(recorddict)
            query_results.append(recorddict)
            n += 1
    except Exception as e:
        print("SQL error for %s" % topic)
        print(e)
        print(sqlquery_real)
        sys.stdout.flush()
        return []

    return query_results

def dispose_query_results(query, query_results):
    """ Send out the query results by email or kafka, and ipdate the digest file
    """
    if len(query_results) == 0:
        return 0
    active = query['active']
    digest,last_entry,last_email = fetch_digest(query['topic_name'])
    utcnow = datetime.datetime.utcnow()
    allrecords = (query_results + digest)[:10000]

    if active == 1:
        # send results by email if 24 hurs has passed, returns time of last email send
        last_email = dispose_email(allrecords, last_email)

    if active == 2:
        # send results by kafka on given topic
        dispose_kafka(query_results, query['topic_name'])

    # update the digest file
    utcnow_text = utcnow.strftime("%Y-%m-%d %H:%M:%S")
    last_email_text = last_email.strftime("%Y-%m-%d %H:%M:%S")
    digest_dict = {
            'last_entry': utcnow_text, 
            'last_email':last_email_text, 
            'digest':allrecords
            }
    digestdict_text = json.dumps(digest_dict, indent=2, default=datetime_converter)

    filename = settings.KAFKA_STREAMS + query['topic_name']
    f = open(filename, 'w')
    os.chmod(filename, 0O666)
    f.write(digestdict_text)
    f.close()
    return len(query_results)

def fetch_digest(topic_name):
    filename = settings.KAFKA_STREAMS + topic_name
    try:
        digest_file = open(filename, 'r')
        digest_dict = json.loads(digest_file.read())
        digest      = digest_dict['digest']
        last_entry_text = digest_dict['last_entry']
        last_email_text = digest_dict['last_email']
        file.close()
    except:
        digest = []
        last_entry_text = "2017-01-01 00:00:00"
        last_email_text = "2017-01-01 00:00:00"
    last_entry = datetime.datetime.strptime(last_entry_text, "%Y-%m-%d %H:%M:%S")
    last_email = datetime.datetime.strptime(last_email_text, "%Y-%m-%d %H:%M:%S")
    return digest,last_entry,last_email

def dispose_email(allrecords, last_email):
    """ Send out email notifications
    """
    utcnow = datetime.datetime.utcnow()
    delta = (utcnow - last_email)
    delta = delta.days + delta.seconds/86400.0
    # send a message at most every 24 hours
    # delta is number of days since last email went out
    if delta < 1.0:
        return last_email
    print('   --- send email to %s' % email)
    sys.stdout.flush()
    query_url = '%s/query/%d/' % (settings.LASAIR_URL, query['mq_id'])
    message      = 'Your active query with Lasair on topic %s\n' % topic
    message_html = 'Your active query with Lasair on <a href=%s>%s</a><br/>' % (query_url, topic)
    for out in allrecords: 
        out_time = datetime.datetime.strptime(out['UTC'], "%Y-%m-%d %H:%M:%S")
        # gather all records that have accumulated since last email
        if out_time > last_email_time:
            if 'objectId' in out:
                objectId = out['objectId']
                message      += objectId + '\n'
                message_html += '<a href="%s/object/%s/">%s</a><br/>' % (settings.LASAIR_URL, objectId, objectId)
            else:
                jsonout = json.dumps(out, default=datetime_converter)
                message += jsonout + '\n'
    try:
        send_email(email, topic, message, message_html)
        return utcnow
    except Exception as e:
        print('ERROR in filter/run_active_queries: Cannot send email!')
        print(e)
        sys.stdout.flush()
        return last_email

def send_email(email, topic, message, message_html):
    """send_email.

    Args:
        email:
        topic:
        message:
    """
    msg = MIMEMultipart('alternative')

    msg['Subject'] = 'Lasair query ' + topic
    msg['From']    = 'donotreply@lasair.roe.ac.uk'
    msg['To']      = email

    msg.attach(MIMEText(message, 'plain'))
    msg.attach(MIMEText(message_html, 'html'))
    s = smtplib.SMTP('localhost')
    s.sendmail('donotreply@lasair.roe.ac.uk', email, msg.as_string())
    s.quit()

def dispose_kafka(query_results, topic):
    """ Send out query results by kafka to the given topic.
    """
    conf = {
        'bootstrap.servers': settings.KAFKA_PRODUCER,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': 'admin',
        'sasl.password': settings.KAFKA_PASSWORD
    }

    try:
        p = Producer(conf)
        for out in query_results: 
            jsonout = json.dumps(out, default=datetime_converter)
            p.produce(topic, value=jsonout, callback=kafka_ack)
        p.flush(10.0)   # 10 second timeout
    except Exception as e:
        rtxt = "ERROR in filter/run_active_queries: cannot produce to public kafka"
        rtxt += str(e)
        slack_webhook.send(settings.SLACK_URL, rtxt)
        print(rtxt)
        sys.stdout.flush()

def db_connect_remote():
    config = {
        'user'    : settings.DB_USER_REMOTE,
        'password': settings.DB_PASS_REMOTE,
        'host'    : settings.DB_HOST_REMOTE,
        'port'    : settings.DB_PORT_REMOTE,
        'database': 'ztf'
    }
    return mysql.connector.connect(**config)

def db_connect_local():
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    return mysql.connector.connect(**config)

def datetime_converter(o):
    """datetime_converter.

    Args:
        o:
    """
# used by json encoder when it gets a type it doesn't understand
    if isinstance(o, datetime.datetime):
        return o.__str__()

def kafka_ack(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))

if __name__ == "__main__":
    sys.path.append('../utility/')
    import slack_webhook
    print('--------- RUN ACTIVE QUERIES -----------')
    sys.stdout.flush()
    t = time.time()
    query_list = fetch_queries()
    run_queries(query_list)
    print('Active queries done in %.1f seconds' % (time.time() - t))
    sys.stdout.flush()
