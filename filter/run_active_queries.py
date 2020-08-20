""" Runs the user's queries. 
Fetch them from database, construct SQL, execute, produce kafka
"""

import time
import json
import settings
import query_utilities
from confluent_kafka import Producer, KafkaError
import datetime
import mysql.connector
import smtplib
from email.message import EmailMessage

def send_email(email, topic, message):
    """send_email.

    Args:
        email:
        topic:
        message:
    """
    msg = EmailMessage()
    msg.set_content(message)

    msg['Subject'] = 'Lasair query ' + topic
    msg['From']    = 'donotreply@lasair.roe.ac.uk'
    msg['To']      = email
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

def datetime_converter(o):
    """datetime_converter.

    Args:
        o:
    """
# used by json encoder when it gets a type it doesn't understand
    if isinstance(o, datetime.datetime):
        return o.__str__()

def run_query(query, msl, topic):
    """run_query.

    Args:
        query:
        msl:
        topic:
    """
    active = query['active']
    email = query['email']
    page = 0
    perpage = 1000
    sqlquery_real = query_utilities.make_query(
        query['selected'], query['tables'], query['conditions'], page, perpage)

    cursor = msl.cursor(buffered=True, dictionary=True)
    n = 0
    recent = []
    try:
        cursor.execute(sqlquery_real)

        #  debug message
        #print('\n%d %f %f\n%s\n' % (active, days_ago_candidates, days_ago_objects, sqlquery_real))

        for record in cursor:
            recorddict = dict(record)
            now_number = datetime.datetime.utcnow()
            recorddict['UTC'] = now_number.strftime("%Y-%m-%d %H:%M:%S")
            #print(recorddict)
            recent.append(recorddict)
            n += 1
    except Exception as e:
        print("Query failed for %s" % topic)
        print(e)
        print(sqlquery_real)

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
            
        allrecords = (recent + digest)[:1000]

        if active == 1:
            # send a message at most every 24 hours
            if delta > 1.0:
                print('   --- send email to %s' % email)
                message = 'Your active query with Lasair on topic ' + topic + '\n'
                for out in allrecords: 
                    out_number = datetime.datetime.strptime(out['UTC'], "%Y-%m-%d %H:%M:%S")
                    # gather all records that have accumulated since last email
                    if out_number > last_entry_number:
                        jsonout = json.dumps(out, default=datetime_converter)
                        message += jsonout + '\n'
                send_email(email, topic, message)
                last_entry_text = now_number.strftime("%Y-%m-%d %H:%M:%S")

        if active == 2:
            conf = { 'bootstrap.servers': settings.KAFKA_PRODUCER }
            try:
                p = Producer(conf)
                for out in recent: 
                    jsonout = json.dumps(out, default=datetime_converter)
                    p.produce(topic, jsonout)
                p.flush(10.0)   # 10 second timeout
                # last_entry not really used with kafka, just a record of last blast
                last_entry_text = now_number.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                print("Kafka production failed for %s" % topic)
                print(e)

        digestdict = {'last_entry': last_entry_text, 'digest':allrecords}
        digestdict_text = json.dumps(digestdict, default=datetime_converter)

        file = open(filename, 'w')
        file.write(digestdict_text)
        file.close()
    return n

def run_queries():
    """run_queries.
    """
    # first get the user queries from the database that the webserver uses
    config = {
        'user'    : settings.DB_USER_REMOTE,
        'password': settings.DB_PASS_REMOTE,
        'host'    : settings.DB_HOST_REMOTE,
        'database': 'ztf'
    }
    msl_remote = mysql.connector.connect(**config)

    cursor   = msl_remote.cursor(buffered=True, dictionary=True)
    query = 'SELECT user, name, email, active, selected, tables, conditions '
    query += 'FROM myqueries, auth_user WHERE myqueries.user = auth_user.id AND active > 0'
    cursor.execute(query)

    query_list = []
    for query in cursor:
        query_dict = {
            'user':      query['user'],
            'name':      query['name'],
            'active':    query['active'],
            'email':     query['email'],
            'selected':  query['selected'],
            'tables':    query['tables'],
            'conditions':query['conditions'],
        }
        query_list.append(query_dict)

    # now run those queries on the local objects we have just made
    config = {
        'user'    : settings.DB_USER_LOCAL,
        'password': settings.DB_PASS_LOCAL,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    msl_local = mysql.connector.connect(**config)

    for query in query_list:
        topic = query_utilities.topic_name(query['user'], query['name'])
        t = time.time()
        n = run_query(query, msl_local, topic)
        t = time.time() - t
        print('   %s got %d in %.1f seconds' % (topic, n, t))

if __name__ == "__main__":
    print('--------- RUN ACTIVE QUERIES -----------')
    t = time.time()
    run_queries()
    print('Active queries done in %.1f seconds' % (time.time() - t))
