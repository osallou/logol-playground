import logging
import re

logger = logging.getLogger('logol')

def find_exact(match, part):
    matches = []
    p = re.compile(match)
    for m in p.finditer(part):
        logger.info('%s, %s' % (str(m.start()), str(m.group())))
        matches.append({
            'start': m.start(),
            'end': m.start() + len(match),
            'indel': 0,
            'sub': 0
        })
    return matches
