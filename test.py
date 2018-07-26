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
    'context_vars': {},
    'spacer': False,
    'context': [],
    'step': None,
    'position': 0
}


def send_msg(msg_to, data):
    uid = 'logol:' + uuid.uuid4().hex
    redis_client.set(uid, json.dumps(data))
    channel.queue_declare(queue=msg_to, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=msg_to,
                          body=uid,
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
    print(" [x] Message sent to " + msg_to)


def __stop_processes():
    for k in wf.keys():
        if k.startswith('mod'):
            for v in wf[k]['vars'].keys():
                msg_to = 'logol-' + k + '-' + v
                send_msg(msg_to, {'step': STEP_END})

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue=msg_to, durable=True)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


uid = 'logol:' + uuid.uuid4().hex
redis_client.set(uid, json.dumps(result))
redis_client.set('logol:count', 1)
redis_client.set('logol:match', 0)
redis_client.set('logol:ban', 0)

channel.basic_publish(exchange='',
                      routing_key=msg_to,
                      body=uid,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
print(" [x] Message sent")

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


connection.close()
