"""
Redis plugin polls Redis for stats

"""
from __future__ import absolute_import
import logging
import redis

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Celery(base.Plugin):

    GUID = 'com.capturedhq.newrelic_celery_agent'

    DEFAULT_PORT = 6379

    def poll(self):
        for broker in self.config.get('brokers', []):
            # connect to redis on each db
            conn = redis.Redis(self.config.get('host'), self.config.get('port', self.DEFAULT_PORT), broker.get('db', 0))
            # run llen for each queue
            for queue in broker.get('queues', []):
                queue_len = conn.llen(queue)
                LOGGER.info('Queue/%s_%s: %d',broker.get('name'),queue,queue_len)
                self.add_gauge_value('Queue/%s_%s'%(broker.get('name'),queue), 'length', queue_len)

