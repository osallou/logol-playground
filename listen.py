#!/usr/bin/env python
import copy
import json
import logging
import os
import pika
import sys
# import time
import uuid

import yaml
import redis

import logol.grammar as g


logging.basicConfig(level=logging.INFO)

logger = logging.getLogger('logol')
logger.setLevel(logging.DEBUG)

sequence = 'cccccaaaaaacgtttttt'


STEP_NONE = g.STEP_NONE
STEP_PRE = g.STEP_PRE
STEP_POST = g.STEP_POST
STEP_END = g.STEP_END


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


model = os.environ.get('LOGOL_MODEL', None)
modelVar = os.environ.get('LOGOL_VAR', None)
runId = os.environ.get('LOGOL_ID', 0)

if not model or not modelVar:
    logger.error('model or modelVar not defined')
    sys.exit(1)

queue = 'logol-%s-%s' % (model, modelVar)

ban_status = False

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost')
)
channel = connection.channel()
channel.queue_declare(queue=queue, durable=True)

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

logger.info(' [*] Waiting for messages. To exit press CTRL+C')


def __get_value(start, length):
    return sequence[start: start + length]


def __find(data, context_vars=[], spacer=False):
    curVar = wf[model]['vars'][modelVar]
    if (
        not curVar['value'] and
        curVar.get('string_constraints', None) and
        curVar['string_constraints'].get('content', None)
    ):
        content_constraint = curVar['string_constraints']['content']
        curVar['value'] = context_vars[content_constraint]['value']
    results = []
    if curVar['value']:
        logger.debug(" [x] search " + str(curVar['value']))
        matches = g.find_exact(curVar['value'], sequence)
        if not matches:
            return None
        ban = 0
        for m in matches:
            if not spacer:
                # should control minposition
                logger.debug(
                    'check minpos %d against match pos %d' %
                    (data.minPosition, m['start'])
                )
                if m['start'] != data.minPosition:
                    logger.debug('skip match ' + json.dumps(m))
                    ban += 1
                    continue
            match = Match()
            match.match['id'] = modelVar
            match.match['model'] = model
            match.match['start'] = m['start']
            match.match['end'] = m['end']
            match.match['info'] = curVar['value']
            # TODO to be removed, for debug only
            __get_value(m['start'],  len(curVar['value']))
            match.match['value'] = __get_value(m['start'],  len(curVar['value']))
            results.append(match)
            logger.debug('Matches: ' + json.dumps(match.match))
        logger.debug(" [x] got matches " + str(len(matches) - ban))

    return results


def send_msg(msg_to, data):
    # TODO add runId to msg_to and use same for queue definition
    uid = 'logol:' + uuid.uuid4().hex
    # sort matches by start position
    data['matches'].sort(key=lambda x: x['start'])
    json_data = json.dumps(data)
    logger.debug("send msg " + json_data)
    redis_client.set(uid, json_data)
    channel.queue_declare(queue=msg_to, durable=True)
    channel.basic_publish(exchange='',
                          routing_key=msg_to,
                          body=uid,
                          properties=pika.BasicProperties(
                             delivery_mode=2,  # make message persistent
                          ))
    logger.debug(" [x] Message sent to " + msg_to)


def go_next(result):
    nextVars = wf[model]['vars'][modelVar]['next']
    if not nextVars:
        # Should check if there is a from to go back to calling model
        if result['from']:
            (back_model, back_var) = result['from'].pop().split('.')
            result['step'] = STEP_POST
            # set output vars
            result['outputs'] = []
            for modelOutput in wf[model]['params']['outputs']:
                result['outputs'].append(result['context_vars'][-1][modelOutput])

            msg_to = 'logol-%s-%s' % (back_model, back_var)
            send_msg(msg_to, result)
        else:
            result['outputs'] = []
            result['iteration'] = 1
            for modelOutput in wf[model]['params']['outputs']:
                    result['outputs'].append(result['context_vars'][-1][modelOutput])
            send_msg('logol-result', result)
    else:
        result['iteration'] = 1
        for nextVar in nextVars:
            msg_to = 'logol-%s-%s' % (model, nextVar)
            send_msg(msg_to, result)


def call_model(result, contextVars=None):
    curVar = wf[model]['vars'][modelVar]
    callModel = curVar['model']['name']
    msg_to = 'logol-%s-%s' % (callModel, wf[callModel]['start'])
    new_result = {
                    'from': [],
                    'matches': [],
                    'context_vars': [],
                    'spacer': False,
                    'context': [],
                    'step': STEP_PRE,
                    'position': 0,
                    'iteration': result.get('iteration', 0) + 1
    }
    new_result['from'] = result['from']
    result['from'] = []
    new_result['from'].append(model + '.' + modelVar)
    new_result['context_vars'] = result['context_vars']
    new_result['context'].append(result['matches'])
    new_result['matches'] = []
    new_result['spacer'] = result['spacer']
    new_result['position'] = result['position']
    new_result['inputs'] = []
    new_result['outputs'] = []
    if contextVars:
        for modelInput in curVar['model']['inputs']:
            new_result['inputs'].append(contextVars[modelInput])
    send_msg(msg_to, new_result)


def callback(ch, method, properties, body):
    global ban_status
    logger.debug(" [x] Received %r" % body)
    bodydata = redis_client.get(body)
    if bodydata is None:
        logger.debug('no body found')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    redis_client.delete(body)
    result = json.loads(bodydata.decode('UTF-8'))
    if result['step'] == g.STEP_BAN:
        ban_status = True
        logger.info("BAN request receiving, skipping messages")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    if result['step'] == STEP_END:
        logger.warn('received stop message, exiting...')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        sys.exit(0)

    if ban_status:
        redis_client.incr('logol:ban')
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    logger.debug("Receive msg: " + json.dumps(result))

    newContextVars = {}
    # if we start a new model or come back to model
    if modelVar == wf[model]['start']:
        if (
            wf[model].get('params', None) and
            result.get('inputs', None) and
            wf[model]['params'].get('inputs', None)
        ):
            # has input parameters
            newContextVars = {}
            for i in range(0, len(wf[model]['params']['inputs'])):
                inputId = wf[model]['params']['inputs'][i]
                newContextVars[inputId] = result['inputs'][i]
        result['context_vars'].append(newContextVars)
        result['inputs'] = []

    contextVars = result['context_vars'][-1]
    logger.debug('context vars: ' + json.dumps(contextVars))

    if result['step'] == STEP_POST:
        # set back context , insert result as children and go to next var
        prev_context = result['context'].pop()
        result['context_vars'].pop()
        if (
            wf[model].get('params', None) and
            result.get('outputs', None) and
            wf[model]['params'].get('outputs', None)
        ):
            # has output parameters
            for i in range(0, len(wf[model]['params']['outputs'])):
                outputId = wf[model]['params']['outputs'][i]
                contextVars[outputId] = result['outputs'][i]
        result['outputs'] = []

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
        result['matches'] = prev_context
        result['matches'].append(match.match)
        result['step'] = STEP_NONE
        result['position'] = match.match['end']
        go_next(result)
        curVar = wf[model]['vars'][modelVar]
        logger.debug(
            'model iteration status %d:%d:%d' %
            (
                result.get('iteration', 0),
                curVar['model'].get('repeatMin', 1),
                curVar['model'].get('repeatMax', 1)
            )
        )

        if (
            result['iteration'] <= curVar['model'].get('repeatMax', 1)
        ):
            logger.info('model iteration %d' % (result.get('iteration', 0)))
            redis_client.incr('logol:count', 1)
            call_model(result, contextVars=result['context_vars'][-1])

    else:
        match = Match()
        curVar = wf[model]['vars'][modelVar]

        # if next is a model/view should add a *from* model
        if curVar.get('model', None):
            logger.debug("call a model")
            call_model(result, contextVars=contextVars)
            '''
            callModel = curVar['model']['name']
            msg_to = 'logol-%s-%s' % (callModel, wf[callModel]['start'])
            new_result = {
                    'from': [],
                    'matches': [],
                    'context_vars': [],
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
            # result['context_vars']
            new_result['inputs'] = []
            new_result['outputs'] = []
            for modelInput in curVar['model']['inputs']:
                new_result['inputs'].append(contextVars[modelInput])

            send_msg(msg_to, new_result)
            '''
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        match.minPosition = result['position']
        matches = __find(match, context_vars=contextVars, spacer=result['spacer'])
        nextVars = wf[model]['vars'][modelVar]['next']
        nbNext = 0

        if not matches:
            redis_client.incr('logol:ban')
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if nextVars:
            nbNext = len(nextVars)
        if nbNext:
            incCount = (nbNext * len(matches)) - 1
            redis_client.incr('logol:count', incCount)
        else:
            incCount = len(matches) - 1
            redis_client.incr('logol:count', incCount)

        prev_matches = result['matches']
        prev_from = result['from']

        result['spacer'] = False
        for m in matches:
            result['from'] = copy.deepcopy(prev_from)
            result['position'] = m.match['end']
            result['matches'] = prev_matches + [m.match]
            if (
                curVar.get('string_constraints', None) and
                curVar['string_constraints'].get('save_as', None)
            ):
                save_as = curVar['string_constraints']['save_as']
                contextVars[save_as] = m.match
            go_next(result)
    logger.debug(" [x] Done")

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=queue)

channel.start_consuming()
