import json
import logging
import os
import sys
import time
import uuid

import yaml
import pika
import redis
logging.basicConfig(level=logging.INFO)

wf = None
with open("test.yml", 'r') as stream:
    try:
        wf = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)


sequence = 'cccccaaaaaagtttttt'

msg_to = 'logol-mod1-var1'

STEP_END = 2

result = {
    'from': [],
    'matches': [],
    'context_vars': [],
    'spacer': True,
    'context': [],
    'step': None,
    'position': 0
}


def send_msg(msg_to, data):
    uid = 'logol:' + uuid.uuid4().hex
    data['msg_to'] = msg_to
    redis_client.set(uid, json.dumps(data))
    channel.basic_publish(exchange='',
                          routing_key='logol-analyse',
                          body=uid,
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
    print(" [x] Message sent to " + msg_to)


def send_event(data):
    # uid = 'logol:' + uuid.uuid4().hex
    # redis_client.set(uid, json.dumps(data))
    channel.basic_publish(exchange='logol-event-exchange',
                          routing_key='',
                          body=json.dumps(data),
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
    print(" [x] Message sent to " + msg_to)


def __stop_processes():
    send_event({'step': STEP_END})
    '''
    send_msg('logol-result', {'step': STEP_END})
    for k in wf.keys():
        if k.startswith('mod'):
            for v in wf[k]['vars'].keys():
                msg_to = 'logol-' + k + '-' + v
                send_msg(msg_to, {'step': STEP_END})
    '''

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

# compute event queue
channel = connection.channel()
channel.queue_declare(queue='logol-analyse', durable=True)

# global events exchange and queue
channel.exchange_declare('logol-event-exchange', exchange_type='fanout')
event_queue = channel.queue_declare(exclusive=True)
channel.queue_bind(exchange='logol-event-exchange',
                   queue=event_queue.method.queue)


redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)



redis_client.set('logol:count', 1)
redis_client.set('logol:match', 0)
redis_client.set('logol:ban', 0)

send_msg(msg_to, result)
'''
uid = 'logol:' + uuid.uuid4().hex
redis_client.set(uid, json.dumps(result))
channel.basic_publish(exchange='',
                      routing_key=msg_to,
                      body=uid,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
print(" [x] Message sent to %s" % (msg_to))
'''


not_over = True
while not_over:
        count = int(redis_client.get('logol:count'))
        ban = redis_client.get('logol:ban')
        if not ban:
            ban = 0
        else:
            ban = int(ban)
        countMatches = int(redis_client.get('logol:match'))
        logging.info('logol:count:' + str(count))
        logging.info('logol:ban:' + str(ban))
        logging.info('logol:match:' + str(countMatches))
        if countMatches + ban == count:
            logging.warn('OVER')
            __stop_processes()
            not_over = False
        time.sleep(2)

channel.exchange_delete('logol-event-exchange')
channel.queue_delete('logol-analyse')
connection.close()
