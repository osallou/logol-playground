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

#logging.basicConfig(level=logging.INFO)

sequence = 'cccccaaaaaacgtttttt'

result = {
    'from': [],
    'matches': []
}

STEP_NONE = -1
STEP_PRE = 0
STEP_POST = 1
STEP_END = 2

class Match(object):
    def __init__(self, match=None, position=0):
        if match is None:
            self.match = {
                'id': None,
                'model': None,
                'uid': None,
                'start': 0,
                'end': 0,
                'sub': 0,
                'indel': 0,
                'info': None,
                'children': [],
                'minRepeat': 1,
                'maxRepeat': 1
            }
        else:
            self.match = match
        self.minPosition = position

    def get(self, attr):
        return self.match.get(attr, None)

    def __str__(self):
        return str(self.match)

    def loads(self, match):
        self.match = json.loads(match)

    def dumps(self):
        return json.dumps(self.match)

    def clone(self):
        return Match().loads(self.dumps())

    def length(self):
        return self.match['end'] - self.match['start']

wf = None
with open("test.yml", 'r') as stream:
    try:
        wf = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)


model = os.environ.get('LOGOL_MODEL',None)
modelVar = os.environ.get('LOGOL_VAR',None)
runId = os.environ.get('LOGOL_ID', 0)

if not model or not modelVar:
    logging.error('model or modelVar not defined')
    sys.exit(1)

queue = 'logol-%s-%s' % (model, modelVar)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

logging.info(' [*] Waiting for messages. To exit press CTRL+C')


def __find(data, vars=[]):
    curVar = wf[model]['vars'][modelVar]
    if not curVar['value'] and curVar.get('string_constraints', None) and curVar['string_constraints'].get('content', None):
        curVar['value'] = vars[curVar['string_constraints']['content']]['value']
    if curVar['value']:
        logging.info(" [x] search " + str(curVar['value']))
        posMatch = sequence.find(curVar['value'])
        if posMatch < 0:
            return None
        logging.info(" [x] got a match at " + str(posMatch))
        data.match['id'] = modelVar
        data.match['model'] = model
        data.match['start'] = posMatch
        data.match['end'] = posMatch + len(curVar['value'])
        data.match['info'] = curVar['value']
        # to be removed
        data.match['value'] = sequence[posMatch:posMatch + len(curVar['value'])]
    return  [data]


def print_result(data):
    logging.warn("Match!")
    logging.info('Result: ' + json.dumps(data))


def send_msg(msg_to, data):
    #TODO add runId to msg_to and use same for queue definition
    uid = 'logol:' + uuid.uuid4().hex
    redis_client.set(uid, json.dumps(data))
    channel.queue_declare(queue=msg_to, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=msg_to,
                          body=uid,
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
    logging.info(" [x] Message sent to " + msg_to)


def go_next(result):
    nextVars = wf[model]['vars'][modelVar]['next']
    if not nextVars:
        # Should check if there is a from to go back to calling model (pop result from)
        if result['from']:
            (back_model, back_var) = result['from'].pop().split('.')
            result['step'] = STEP_POST
            msg_to = 'logol-%s-%s' % (back_model, back_var)
            send_msg(msg_to, result)
        else:
            send_msg('logol-result', result)
            #print_result(result)
            #redis_client.incr('logol:match', 1)

    else:
        '''
        redis_client.incr('logol:count', len(nextVars) -1)
        logging.warn('incr logol:count')
        '''
        for nextVar in nextVars:
            msg_to = 'logol-%s-%s' % (model, nextVar)
            send_msg(msg_to, result)


def callback(ch, method, properties, body):
    logging.info(" [x] Received %r" % body)
    bodydata = redis_client.get(body)
    if bodydata is None:
        logging.error('no body found')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return

    redis_client.delete(body)
    result = json.loads(bodydata)
    if result['step'] == STEP_END:
        logging.warn('received stop message, exiting...')
        ch.basic_ack(delivery_tag = method.delivery_tag)
        sys.exit(0)
    if result['step'] == STEP_POST:
        # set back context , insert result as children and go to next var
        prev_context = result['context'].pop()
        match = Match()
        match.match['model'] = model
        match.match['id'] = modelVar
        for m in result['matches']:
            if match.match['start'] == 0 or m['start'] < match.match['start']:
                match.match['start'] = m['start']
            if match.match['end'] == 0 or m['end'] > match.match['end']:
                match.match['end'] = m['end']
            match.match['sub'] += m['sub']
            match.match['indel'] += m['indel']
        match.match['children'] = result['matches']
        # build match from matches
        result['matches']= prev_context
        result['matches'].append(match.match)
        result['step'] = STEP_NONE
        result['position'] = match.match['end']
        go_next(result)
    else:
        match = Match()
        curVar = wf[model]['vars'][modelVar]

        # if next is a model/view should add a *from* model
        if curVar.get('model', None):
            logging.info("call a model")
            msg_to = 'logol-%s-%s' % (curVar['model'], wf[curVar['model']]['start'])
            new_result = {
                    'from': [],
                    'matches': [],
                    'context_vars': {},
                    'spacer': False,
                    'context': [],
                    'step': STEP_PRE,
                    'position': 0
            }
            new_result['from'] = result['from']
            result['from'] = []
            new_result['from'].append(model + '.' + modelVar)
            new_result['context_vars'] = result['context_vars']
            new_result['context'].append(result['matches'])
            new_result['matches'] = []
            new_result['spacer'] = result['spacer']
            new_result['position'] = result['position']
            send_msg(msg_to, new_result)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return


        match.minPosition = result['position']
        matches = __find(match,vars=result['context_vars'])
        nextVars = wf[model]['vars'][modelVar]['next']
        nbNext = 0
        if nextVars:
            nbNext = len(nextVars)
        if nbNext:
            incCount = (nbNext * len(matches)) - 1
            redis_client.incr('logol:count', incCount)
        else:
            incCount = len(matches) - 1
            redis_client.incr('logol:count', incCount)
        if not matches:
            redis_client.incr('logol:ban')
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return
        prev_matches = result['matches']
        prev_from = result['from']
        for m in matches:
            result['from'] = copy.deepcopy(prev_from)
            result['position'] = m.match['end']
            result['matches'] = prev_matches + [m.match]
            if curVar.get('string_constraints', None) and curVar['string_constraints'].get('save_as', None):
                save_as = curVar['string_constraints']['save_as']
                result['context_vars'][save_as] = m.match
            go_next(result)
    logging.info(" [x] Done")

    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=queue)

channel.start_consuming()

#callback(None, None, None, json.dumps(result))
