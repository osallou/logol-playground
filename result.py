#!/usr/bin/env python
import json
import logging
import os
import pika
import sys
import uuid

import yaml
import redis

import logol.grammar as g

STEP_END = 2

queue = 'logol-result'

max_results = int(os.environ.get('LOGOL_MAX_RES', 1000))
nb_results = 0

wf = None
with open("test.yml", 'r') as stream:
    try:
        wf = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

logging.basicConfig(level=logging.INFO)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

result_file = open('logol.out', 'w')


def send_msg(msg_to, data):
    uid = 'logol:' + uuid.uuid4().hex
    redis_client.set(uid, json.dumps(data))
    channel.queue_declare(queue=msg_to, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=msg_to,
                          body=uid,
                          properties=pika.BasicProperties(
                             delivery_mode=2,  # make message persistent
                          ))
    print(" [x] Message sent to " + msg_to)


def send_ban():
    for k in wf.keys():
        if k.startswith('mod'):
            for v in wf[k]['vars'].keys():
                msg_to = 'logol-' + k + '-' + v
                send_msg(msg_to, {'step': g.STEP_BAN})


def callback(ch, method, properties, body):
    global nb_results
    logging.info(" [x] Received %r" % body)
    bodydata = redis_client.get(body)
    if bodydata is None:
        logging.error('no body found')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    redis_client.delete(body)
    result = json.loads(bodydata.decode('UTF-8'))

    if result['step'] == STEP_END:
        logging.warn('received stop message, exiting...')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        result_file.close()
        sys.exit(0)

    logging.info("Res: " + json.dumps(result))
    if nb_results < max_results:
        result_file.write(json.dumps(result['matches']) + "\n")
        redis_client.incr('logol:match', 1)
    else:
        logging.warn('Max results reached [%d], waiting to end...' % (max_results))
        redis_client.incr('logol:ban', 1)
        send_ban()

    nb_results += 1

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=queue)

channel.start_consuming()
