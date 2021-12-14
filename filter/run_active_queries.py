""" Runs the user's queries. 
Fetch them from database, construct SQL, execute, produce kafka
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

def run_query(query, msl, annotator=None, objectId=None):
    """run_query. Two cases here: 
    if annotator=None, runs the query against the local database
    if annotator and objectId, checks if the query involves the annotator, 
        and if so, runs the query for the given object

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
            return 0
        # run the query against master for this specific object that has been annotated
        tok = sqlquery_real.split(' ORDER BY ')
        sqlquery_real = tok[0] + (' AND objects.objectId="%s" ' % objectId)
        if len(tok) == 2: # has order clause, add it back
            sqlquery_real += ' ORDER BY ' + tok[1]
        #print('\n' + sqlquery_real)
    sqlquery_real += (' LIMIT %d' % limit)

    cursor = msl.cursor(buffered=True, dictionary=True)
    n = 0
    recent = []
    try:
        cursor.execute(sqlquery_real)

        for record in cursor:
            recorddict = dict(record)
            now_number = datetime.datetime.utcnow()
            recorddict['UTC'] = now_number.strftime("%Y-%m-%d %H:%M:%S")
            #print(recorddict)
            recent.append(recorddict)
            n += 1
    except Exception as e:
        print("SQL error for %s" % topic)
        print(e)
        print(sqlquery_real)
        sys.stdout.flush()
        return 0

    #print(recent)
    if len(recent) > 0:
        filename = settings.KAFKA_LASAIR_LOGS + topic
        try:
            file = open(filename, 'r')
            digestdict = json.loads(file.read())
            digest     = digestdict['digest']
            last_entry_text = digestdict['last_entry']
            file.close()
        except:
            digest = []
            last_entry_text = "2017-01-01 00:00:00"

        last_entry_number = datetime.datetime.strptime(last_entry_text, "%Y-%m-%d %H:%M:%S")
        now_number = datetime.datetime.utcnow()
        delta = (now_number - last_entry_number)
        delta = delta.days + delta.seconds/86400.0
            
        allrecords = (recent + digest)[:10000]

        if active == 1:
            # send a message at most every 24 hours
            if delta > 1.0:
                print('   --- send email to %s' % email)
                sys.stdout.flush()
                query_url = '%s/query/%d/' % (settings.LASAIR_URL, query['mq_id'])
                message      = 'Your active query with Lasair on topic %s\n' % topic
                message_html = 'Your active query with Lasair on <a href=%s>%s</a><br/>' % (query_url, topic)
                for out in allrecords: 
                    out_number = datetime.datetime.strptime(out['UTC'], "%Y-%m-%d %H:%M:%S")
                    # gather all records that have accumulated since last email
                    if out_number > last_entry_number:
                        if 'objectId' in out:
                            objectId = out['objectId']
                            message      += objectId + '\n'
                            message_html += '<a href="%s/object/%s/">%s</a><br/>' % (settings.LASAIR_URL, objectId, objectId)
                        else:
                            jsonout = json.dumps(out, default=datetime_converter)
                            message += jsonout + '\n'
                try:
                    send_email(email, topic, message, message_html)
                except Exception as e:
                    print('ERROR in filter/run_active_queries: Cannot send email!')
                    print(e)
                    sys.stdout.flush()

                last_entry_text = now_number.strftime("%Y-%m-%d %H:%M:%S")

        if active == 2:
            conf = {
                'bootstrap.servers': settings.KAFKA_PRODUCER,
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanisms': 'SCRAM-SHA-256',
                'sasl.username': 'admin',
                'sasl.password': settings.KAFKA_PASSWORD
            }

            try:
                p = Producer(conf)
                for out in recent: 
                    jsonout = json.dumps(out, default=datetime_converter)
                    p.produce(topic, value=jsonout, callback=kafka_ack)
                p.flush(10.0)   # 10 second timeout
                # last_entry not really used with kafka, just a record of last blast
                last_entry_text = now_number.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                rtxt = "ERROR in filter/run_active_queries: cannot produce to public kafka"
                rtxt += str(e)
                slack_webhook.send(settings.SLACK_URL, rtxt)
                print(rtxt)
                sys.stdout.flush()

        digestdict = {'last_entry': last_entry_text, 'digest':allrecords}
        digestdict_text = json.dumps(digestdict, indent=2, default=datetime_converter)

        file = open(filename, 'w')
        os.chmod(filename, 0O666)
        file.write(digestdict_text)
        file.close()
    return n

def run_queries(annotation_list = None):
    """run_queries.
    When annotation_list is None, it runs all the queries against the local database
    When not None, runs some queires agains a specific object, using the master database
    """
    # first get the user queries from the database that the webserver uses
    config = {
        'user'    : settings.DB_USER_REMOTE,
        'password': settings.DB_PASS_REMOTE,
        'host'    : settings.DB_HOST_REMOTE,
        'port'    : settings.DB_PORT_REMOTE,
        'database': 'ztf'
    }
    msl_remote = mysql.connector.connect(**config)

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

    # now run those queries on the local objects we have just made
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    try:
        msl_local = mysql.connector.connect(**config)
    except:
        print('ERROR in filter/run_active_queries: cannot connecto to local database')
        sys.stdout.flush()

    for query in query_list:
        n = 0
        t = time.time()

        # normal case of streaming queries
        if not annotation_list:  
            n += run_query(query, msl_local)

        # immediate response to active=2 annotators
        else:
            for ann in annotation_list:  
                n += run_query(query, msl_remote, ann['annotator'], ann['objectId'])

        t = time.time() - t
        if n > 0:
            print('   %s got %d in %.1f seconds' % (query['topic_name'], n, t))
            sys.stdout.flush()

def run_annotation_queries():
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
    run_queries(annotation_list)

if __name__ == "__main__":
    print('--------- RUN ACTIVE QUERIES -----------')
    sys.stdout.flush()
    t = time.time()
    run_queries()
    print('Active queries done in %.1f seconds' % (time.time() - t))
    sys.stdout.flush()
