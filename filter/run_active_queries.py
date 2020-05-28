import time
import json
import settings
from query_utilities import make_query
from confluent_kafka import Producer, KafkaError
import datetime

candidates = 'candidates'
objects    = 'objects'

# setup database connection
import mysql.connector

import smtplib
from email.message import EmailMessage
def send_email(email, topic, message):
    msg = EmailMessage()
    msg.set_content(message)

    msg['Subject'] = 'Lasair query ' + topic
    msg['From']    = 'donotreply@lasair.roe.ac.uk'
    msg['To']      = email
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()

def datetime_converter(o):
# used by json encoder when it gets a type it doesn't understand
    if isinstance(o, datetime.datetime):
        return o.__str__()

def run_query(query, msl, active, email, topic):
    sqlquery_real = make_query(
            query['selected'], query['tables'], query['conditions'], 0, 1000, False, 0.0)

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
        filename = '/data/ztf/streams/%s' % topic
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
        print('   --- previous entry %.4f days ago' % delta)
            
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
            conf = { 'bootstrap.servers': settings.LASAIR_KAFKA_PRODUCER }
            try:
                p = Producer(conf)
                for out in recent: 
                    jsonout = json.dumps(out, default=datetime_converter)
                    p.produce(topic, jsonout)
                p.flush(10.0)   # 10 second timeout
                # last_entry not really used with kafka, just a record of last blast
                last_entry_text = now_number.strftime("%Y-%m-%d %H:%M:%S")
                print('    -- sent to kafka')
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
    config = {
        'user'    : settings.DB_USER_WRITE,
        'password': settings.DB_PASS_WRITE,
        'host'    : settings.DB_HOST_LOCAL,
        'database': 'ztf'
    }
    msl = mysql.connector.connect(**config)

    cursor   = msl.cursor(buffered=True, dictionary=True)
    query = 'SELECT user, name, email, active, selected, tables, conditions '
    query += 'FROM myqueries, auth_user WHERE myqueries.user = auth_user.id AND active > 0'
    cursor.execute(query)

    for query in cursor:
        topic = queries.topic_name(query['user'], query['name'])
        print('query %s' % topic)
        active = query['active']
        email = query['email']
        t = time.time()
        n = run_query(query, msl, active, email, topic)
        t = time.time() - t
        print('   --- got %d in %.1f seconds' % (n, t))

if __name__ == "__main__":
    print('--------- RUN ACTIVE QUERIES -----------')
    t = time.time()
    run_queries()
    print('Active queries done in %.1f seconds' % (time.time() - t))
