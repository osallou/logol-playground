#!/usr/bin/env python
import copy
import json
import logging
import os
import pika
import sys
import time
import uuid

import yaml
import redis

STEP_END = 2

queue = 'logol-result'

logging.basicConfig(level=logging.INFO)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

result_file = open('logol.out', 'w')

def callback(ch, method, properties, body):
    logging.info(" [x] Received %r" % body)
    bodydata = redis_client.get(body)
    if bodydata is None:
        logging.error('no body found')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return

    redis_client.delete(body)
    result = json.loads(bodydata.decode('UTF-8'))

    if result['step'] == STEP_END:
        logging.warn('received stop message, exiting...')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        result_file.close()
        sys.exit(0)

    logging.info("Res: " + json.dumps(result['matches']))
    result_file.write(json.dumps(result['matches']) + "\n")
    redis_client.incr('logol:match', 1)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=queue)

channel.start_consuming()
