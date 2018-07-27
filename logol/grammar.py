import logging
# import re


logger = logging.getLogger('logol')


STEP_NONE = -1
STEP_PRE = 0
STEP_POST = 1
STEP_END = 2
STEP_BAN = 3

def find_exact(match, part, pos=None):
    # naive search only for testing
    matches = []
    minStart = 0
    if pos:
        minStart = pos
    for i in range(minStart, len(part)):
        word = part[i:i + len(match)]
        if match == word:
            matches.append({
                'start': i,
                'end': i + len(match),
                'indel': 0,
                'sub': 0
            })
        if pos:
            break
    '''
    p = re.compile(match)
    for m in p.finditer(part, overlapped=True):
        logger.info('%s, %s' % (str(m.start()), str(m.group())))
        matches.append({
            'start': m.start(),
            'end': m.start() + len(match),
            'indel': 0,
            'sub': 0
        })
    '''
    return matches
