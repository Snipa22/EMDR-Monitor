#!/usr/bin/env python
"""
Consumer based off Greg Taylor's EMDR greenlet consumer
"""
import zlib
# This can be replaced with the built-in json module, if desired.
import simplejson

import gevent
from gevent.pool import Pool
from gevent import monkey; gevent.monkey.patch_all()
import zmq
import PySQLPool
from config import config
from datetime import datetime
import time
import dateutil.parser

PySQLPool.getNewPool().maxActiveConnections = 50

dbConn = PySQLPool.getNewConnection(user=config['username'],passwd=config['password'],db=config['db'], commitOnEnd=True)

# As we're pooling MySQL connections, we're able to run higher counts of greenlets over
# the overall number of available connections to MySQL
MAX_NUM_POOL_WORKERS = 300

def main():
    """
    The main flow of the application.
    """
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Connect to the first publicly available relay.
    subscriber.connect('tcp://relay-us-central-1.eve-emdr.com:8050')
    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")

    # We use a greenlet pool to cap the number of workers at a reasonable level.
    greenlet_pool = Pool(size=MAX_NUM_POOL_WORKERS)

    print("Consumer daemon started, waiting for jobs...")
    print("Worker pool size: %d" % greenlet_pool.size)

    while True:
        # Since subscriber.recv() blocks when no messages are available,
        # this loop stays under control. If something is available and the
        # greenlet pool has greenlets available for use, work gets done.
        greenlet_pool.spawn(worker, subscriber.recv())

def worker(job_json):
    """
    For every incoming message, this worker function is called. Be extremely
    careful not to do anything CPU-intensive here, or you will see blocking.
    Sockets are async under gevent, so those are fair game.
    """
    # Receive raw market JSON strings.
    market_json = zlib.decompress(job_json)
    # Un-serialize the JSON data to a Python dict.
    market_data = simplejson.loads(market_json)

    # Pull the EMDR IP Hash from the system
    for uploaderKey in market_data['uploadKeys']:
        if uploaderKey['name'] == 'EMDR':
            uploadKey = uploaderKey['key']

    # If there's multiple generators, then someone is fail.
    try:
        genName = market_data['generator']['name']
    except KeyError:
        genName = "Unknown"

    try:
        genVersion = market_data['generator']['version']
    except KeyError:
        genVersion = "Unknown"

    genData = genName + " v. " + genVersion


    for row in market_data['rowsets']:
        rowType = 'new'
        genTime = dateutil.parser.parse(row['generatedAt'])
        genTime = int(time.mktime(genTime.timetuple()))
        now = int(time.mktime(time.gmtime()))
        if (now - genTime) > 86400:
            rowType = 'old'
        if len(row['rows']) == 0:
            rowType = 'null'
        queryText = "INSERT INTO monitor (`key`, `generator`, `%s`, `total`) values ('%s', '%s', 1, 1) ON DUPLICATE KEY UPDATE `%s`=`%s`+1, `generator`='%s', `total`=`total`+1" % (rowType, uploadKey, genData, rowType, rowType, genData)
        query = PySQLPool.getNewQuery(dbConn)
        query.Query(queryText)

if __name__ == '__main__':
    main()
