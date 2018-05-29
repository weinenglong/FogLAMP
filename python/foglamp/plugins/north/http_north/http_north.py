# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" HTTP North """

import aiohttp
from aiohttp import web
import json

from foglamp.common import logger
from foglamp.plugins.north.common.common import *

__author__ = "Ashish Jabble"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)


http_north = None
config = ""

# Configuration related to HTTP North
_CONFIG_CATEGORY_NAME = "HTTP_TR"
_CONFIG_CATEGORY_DESCRIPTION = "HTTP North Plugin"

_DEFAULT_CONFIG = {
    'plugin': {
         'description': 'HTTP North Plugin',
         'type': 'string',
         'default': 'http_north'
    },
    'url': {
        'description': 'URI to accept data',
        'type': 'string',
        'default': 'http://localhost:8118/ingress/messages'
    },
    'shutdown_wait_time': {
        'description': 'how long (x seconds) the plugin should wait for pending tasks to complete or cancel otherwise',
        'type': 'integer',
        'default': '10'
    },
    "applyFilter": {
        "description": "Whether to apply filter before processing the data",
        "type": "boolean",
        "default": "False"
    },
    "filterRule": {
        "description": "JQ formatted filter to apply (applicable if applyFilter is True)",
        "type": "string",
        "default": ".[]"
    },
    "max_attempts": {
        "description": "Maximum no. of retries when a packet fails to trasmit",
        "type": "integer",
        "default": "5"
    }

}


# TODO write to Audit Log
def plugin_info():
    return {
        'name': 'http_north',
        'version': '1.0.0',
        'type': 'north',
        'interface': '1.0',
        'config': _DEFAULT_CONFIG
    }


def plugin_init(data):
    global http_north, config
    http_north = HttpNorthPlugin()
    config = data
    return config


def plugin_send(data, payload, stream_id):
    return http_north.send_payloads(payload, stream_id)


def plugin_shutdown(data):
    pass


# TODO: (ASK) North plugin can not be reconfigured? (per callback mechanism)
def plugin_reconfigure():
    pass


class FailedRequest(Exception):
    """
    A wrapper of all possible exception during a HTTP request
    """
    message = ''
    url = ''
    raised = ''

    def __init__(self, *, raised='', message='', url=''):
        self.raised = raised
        self.message = message
        self.url = url

        super().__init__("url={u} message={m} raised={r}".format(u=self.url, m=self.message, r=self.raised))


class HttpNorthPlugin(object):
    """ North HTTP Plugin """

    def __init__(self):
        self.event_loop = asyncio.get_event_loop()
        self.tasks = []

    def send_payloads(self, payloads, stream_id):
        is_data_sent = False
        new_last_object_id = 0
        num_sent = 0
        try:
            new_last_object_id, num_sent = self.event_loop.run_until_complete(self._send_payloads(payloads))
            if num_sent:
                is_data_sent = True
        except Exception as ex:
            _LOGGER.exception("Data could not be sent, %s", str(ex))

        return is_data_sent, new_last_object_id, num_sent

    async def _send_payloads(self, payloads):
        """ send a list of block payloads """
        num_count = 0
        last_id = None

        for payload in payloads:
            try:
                p = {"asset_code": payload['asset_code'],
                     "readings": [{
                         "read_key": payload['read_key'],
                         "user_ts": payload['user_ts'],
                         "reading": payload['reading']
                     }]}
                await self._send(p)
                num_count += 1
                last_id = payload['id']
            except FailedRequest as ex:
                _LOGGER.exception("Data for id %s could not be sent", payload['id'])
            except Exception as ex:
                _LOGGER.exception("Exception %s occurred while sending data for id %s could not be sent", str(ex), payload['id'])

        return last_id, num_count

    async def _send(self, payload):
        """ Send the payload, using ClientSession """
        url = config['url']['value']
        max_attempts = config['max_attempts']['value']
        headers = {'content-type': 'application/json'}
        raised_exc = None
        attempt_count = 0
        backoff_interval = 0.5

        if max_attempts == -1:  # -1 means retry indefinitely
            attempt_count = -1
        elif max_attempts == 0:  # Zero means don't retry
            attempt_count = 1
        else:  # any other value means retry N times
            attempt_count = max_attempts + 1

        while attempt_count != 0:
            try:
                if raised_exc:
                    _LOGGER.error('caught "%s" url:%s, remaining tries %s, sleeping %.2fsecs',
                                  raised_exc, url, attempt_count, backoff_interval)
                    await asyncio.sleep(backoff_interval)

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=json.dumps(payload), headers=headers) as resp:
                        result = await resp.text()
                        status_code = resp.status
                        if status_code in range(400, 500):
                            raise web.HTTPClientError("Bad request error code: %d, reason: %s", status_code, resp.reason)
                        if status_code in range(500, 600):
                            raise web.HTTPServerError("Server error code: %d, reason: %s", status_code, resp.reason)
            except (aiohttp.ClientError, web.HTTPClientError, web.HTTPServerError) as exc:
                raised_exc = FailedRequest(message=exc, url=url, raised=exc.__class__.__name__)
            else:
                raised_exc = None
                break

            attempt_count -= 1

        if raised_exc:
            raise raised_exc
        else:
            return result
