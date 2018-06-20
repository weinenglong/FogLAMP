#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" The sending process is run according to a schedule in order to send reading data
to the historian, e.g. the PI system.
It’s role is to implement the rules as to what needs to be sent and when,
extract the data from the storage subsystem and stream it to the north
for sending to the external system.
The sending process does not implement the protocol used to send the data,
that is devolved to the translation plugin in order to allow for flexibility
in the translation process.
"""

import aiohttp
import resource
import asyncio
import sys
import time
import importlib
import logging
import datetime
import signal
import json

import foglamp.plugins.north.common.common as plugin_common

from foglamp.common.parser import Parser
from foglamp.common.storage_client.storage_client import StorageClient, ReadingsStorageClient, StorageClientAsync, ReadingsStorageClientAsync
from foglamp.common import logger
from foglamp.common.configuration_manager import ConfigurationManager
from foglamp.common.storage_client import payload_builder
from foglamp.common import statistics
from foglamp.common.jqfilter import JQFilter
from foglamp.common.audit_logger import AuditLogger


__author__ = "Stefano Simonelli, Massimiliano Pinto, Mark Riddoch"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


""" Module information """
_MODULE_NAME = "sending_process"
_MESSAGES_LIST = {
    # Information messages
    "i000001": "Started.",
    "i000002": "Execution completed.",
    "i000003": _MODULE_NAME + " disabled.",
    "i000004": "no data will be sent, the stream id is disabled - stream id |{0}|",
    # Warning / Error messages
    "e000000": "general error",
    "e000001": "cannot start the logger - error details |{0}|",
    "e000002": "cannot complete the operation - error details |{0}|",
    "e000003": "cannot complete the retrieval of the configuration",
    "e000004": "cannot complete the initialization - error details |{0}|",
    "e000005": "cannot load the plugin |{0}|",
    "e000006": "cannot complete the sending operation of a block of data.",
    "e000007": "cannot complete the termination of the sending process.",
    "e000008": "unknown data source, it could be only: readings, statistics or audit.",
    "e000009": "cannot load data into memory - error details |{0}|",
    "e000010": "cannot update statistics.",
    "e000011": "invalid input parameters, the stream id is required and it should be a number "
               "- parameters |{0}|",
    "e000012": "cannot connect to the DB Layer - error details |{0}|",
    "e000013": "cannot validate the stream id - error details |{0}|",
    "e000014": "multiple streams having same id are defined - stream id |{0}|",
    "e000015": "the selected plugin is not a valid north plug in type/name |{0} / {1}|",
    "e000016": "invalid stream id, it is not defined - stream id |{0}|",
    "e000017": "cannot handle command line parameters - error details |{0}|",
    "e000018": "cannot initialize the plugin |{0}|",
    "e000019": "cannot retrieve the starting point for sending operation.",
    "e000020": "cannot update the reached position - error details |{0}|",
    "e000021": "cannot complete the sending operation - error details |{0}|",
    "e000022": "unable to convert in memory data structure related to the statistics data "
               "- error details |{0}|",
    "e000023": "cannot complete the initialization - error details |{0}|",
    "e000024": "unable to log the operation in the Storage Layer - error details |{0}|",
    "e000025": "Required argument '--name' is missing - command line |{0}|",
    "e000026": "Required argument '--port' is missing - command line |{0}|",
    "e000027": "Required argument '--address' is missing - command line |{0}|",
    "e000028": "cannot complete the fetch operation - error details |{0}|",
    "e000029": "an error occurred  during the teardown operation - error details |{0}|",

}
""" Messages used for Information, Warning and Error notice """

# LOG configuration
_LOG_LEVEL_DEBUG = 10
_LOG_LEVEL_INFO = 20
_LOG_LEVEL_WARNING = 30

_LOGGER_LEVEL = _LOG_LEVEL_WARNING
_LOGGER_DESTINATION = logger.SYSLOG

_LOGGER = logger.setup(__name__, destination=_LOGGER_DESTINATION, level=_LOGGER_LEVEL)

_event_loop = ""
_log_performance = False
""" Enable/Disable performance logging, enabled using a command line parameter"""


class PluginInitialiseFailed(RuntimeError):
    """ PluginInitializeFailed """
    pass


class UnknownDataSource(RuntimeError):
    """ the data source could be only one among: readings, statistics or audit """
    pass


class InvalidCommandLineParameters(RuntimeError):
    """ Invalid command line parameters, the stream id is the only required """
    pass


def apply_date_format(in_data):
    """ This routine adds the default UTC zone format to the input date time string
    If a timezone (strting with + or -) is found, all the following chars
    are replaced by +00, otherwise +00 is added.

    Note: if the input zone is +02:00 no date conversion is done,
          at the time being this routine expects UTC date time values.

    Examples:
        2018-05-28 16:56:55              ==> 2018-05-28 16:56:55.000000+00
        2018-05-28 13:42:28.84           ==> 2018-05-28 13:42:28.840000+00
        2018-03-22 17:17:17.166347       ==> 2018-03-22 17:17:17.166347+00
        2018-03-22 17:17:17.166347+00:00 ==> 2018-03-22 17:17:17.166347+00
        2018-03-22 17:17:17.166347+00    ==> 2018-03-22 17:17:17.166347+00
        2018-03-22 17:17:17.166347+02:00 ==> 2018-03-22 17:17:17.166347+00

    Args:
        the date time string to format
    Returns:
        the newly formatted datetime string
    """

    # Look for timezone start with '-' a the end of the date (-XY:WZ)
    zone_index = in_data.rfind("-")
    # If index is less than 10 we don't have the trailing zone with -
    if (zone_index < 10):
        #  Look for timezone start with '+' (+XY:ZW)
        zone_index = in_data.rfind("+")

    if zone_index == -1:

        if in_data.rfind(".") == -1:

            # there are no milliseconds in the date
            in_data += ".000000"

        # Pads with 0 if needed
        in_data = in_data.ljust(26, '0')

        # Just add +00
        timestamp = in_data + "+00"
    else:
        # Remove everything after - or + and add +00
        timestamp = in_data[:zone_index] + "+00"

    return timestamp

def _performance_log(func):
    """ Logs information for performance measurement """
    def wrapper(*arg):
        """ wrapper """
        start = datetime.datetime.now()
        # Code execution
        res = func(*arg)

        if _log_performance:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            process_memory = usage.ru_maxrss / 1000
            delta = datetime.datetime.now() - start
            delta_milliseconds = int(delta.total_seconds() * 1000)
            _LOGGER.info("PERFORMANCE - {0} - milliseconds |{1:>8,}| - memory MB |{2:>8,}|"
                         .format(func.__name__,
                                 delta_milliseconds,
                                 process_memory))
        return res
    return wrapper


def handling_input_parameters():
    """ Handles command line parameters
    Returns:
        param_mgt_name: Parameter generated by the scheduler, unique name that represents the microservice.
        param_mgt_port: Parameter generated by the scheduler, Dynamic port of the management API.
        param_mgt_address: Parameter generated by the scheduler, IP address of the server for the management API.
        stream_id: Define the stream id to be used.
        log_performance: Enable/Disable the logging of the performance.
        log_debug_level: Enable/define the level of logging for the debugging 0-3.
    Raises :
        InvalidCommandLineParameters
    """
    _LOGGER.debug("{func} - argv {v0} ".format(
                func="handling_input_parameters",
                v0=str(sys.argv[1:])))
    # Retrieves parameters
    param_mgt_name = Parser.get('--name')
    param_mgt_port = Parser.get('--port')
    param_mgt_address = Parser.get('--address')
    param_stream_id = Parser.get('--stream_id')
    param_performance_log = Parser.get('--performance_log')
    param_debug_level = Parser.get('--debug_level')
    # Evaluates mandatory parameters
    if param_mgt_port is None:
        _message = _MESSAGES_LIST["e000026"].format(str(sys.argv))
        _LOGGER.error(_message)
        raise InvalidCommandLineParameters(_message)
    if param_stream_id is None:
        _message = _MESSAGES_LIST["e000011"].format(str(sys.argv))
        _LOGGER.error(_message)
        raise InvalidCommandLineParameters(_message)
    else:
        try:
            stream_id = int(param_stream_id)
        except Exception:
            _message = _MESSAGES_LIST["e000011"].format(str(sys.argv))
            _LOGGER.error(_message)
            raise InvalidCommandLineParameters(_message)
    # Evaluates optional parameters
    if param_mgt_name is None:
        _message = _MESSAGES_LIST["e000025"].format(str(sys.argv))
        _LOGGER.warning(_message)
    if param_mgt_address is None:
        _message = _MESSAGES_LIST["e000027"].format(str(sys.argv))
        _LOGGER.warning(_message)
    if param_performance_log is not None:
        log_performance = True
    else:
        log_performance = False
    if param_debug_level is not None:
        log_debug_level = int(param_debug_level)
    else:
        log_debug_level = 0

    _LOGGER.debug("{func} "
                  "- name |{name}| - port |{port}| - address |{address}| "
                  "- stream_id |{stream_id}| - log_performance |{perf}| "
                  "- log_debug_level |{debug_level}|".format(
                        func="handling_input_parameters",
                        name=param_mgt_name,
                        port=param_mgt_port,
                        address=param_mgt_address,
                        stream_id=stream_id,
                        perf=log_performance,
                        debug_level=log_debug_level))
    return param_mgt_name, param_mgt_port, param_mgt_address, stream_id, log_performance, log_debug_level


class SendingProcess:
    """ SendingProcess """

    _logger = None  # type: logging.Logger

    _stop_execution = False
    """ sets to True when a signal is captured and a termination is needed """

    TASK_FETCH_SLEEP = 0.5
    """ The amount of time the fetch operation will sleep if there are no more data to load or in case of an error """

    TASK_SEND_SLEEP = 0.5
    """ The amount of time the sending operation will sleep in case of an error """

    TASK_SLEEP_MAX_INCREMENTS = 4
    """ Maximum number of increments for the sleep handling, the amount of time is doubled at every sleep """

    TASK_SEND_UPDATE_POSITION_MAX = 10
    """ the position is updated after the specified numbers of interactions of the sending task """

    # Filesystem path where the norths reside
    _NORTH_PATH = "foglamp.plugins.north."

    # Define the type of the plugin managed by the Sending Process
    _PLUGIN_TYPE = "north"

    # Types of sources for the data blocks
    _DATA_SOURCE_READINGS = "readings"
    _DATA_SOURCE_STATISTICS = "statistics"
    _DATA_SOURCE_AUDIT = "audit"

    # Audit code to use
    _AUDIT_CODE = "STRMN"

    # Configuration retrieved from the Configuration Manager
    _CONFIG_CATEGORY_NAME = 'SEND_PR'
    _CONFIG_CATEGORY_DESCRIPTION = 'Sending Process'
    _CONFIG_DEFAULT = {
        "enable": {
            "description": "Enable execution of the sending process",
            "type": "boolean",
            "default": "True"
        },
        "duration": {
            "description": "Time in seconds the sending process should run",
            "type": "integer",
            "default": "60"
        },
        "sleepInterval": {
            "description": "Time in seconds to wait between duration checks",
            "type": "integer",
            "default": "1"
        },
        "source": {
            "description": "Source of data to be sent on the stream. "
                           "May be either readings, statistics or audit.",
            "type": "string",
            "default": _DATA_SOURCE_READINGS
        },
        "blockSize": {
            "description": "Bytes to send in each transmission",
            "type": "integer",
            "default": "500"
        },
        "memory_buffer_size": {
            "description": "Number of elements of blockSize size to be buffered in memory",
            "type": "integer",
            "default": "10"
        },
        "north": {
            "description": "Name of the north plugin to use to translate readings "
                           "into the output format and send them",
            "type": "string",
            "default": "omf"
        },
        "stream_id": {
            "description": "Stream ID",
            "type": "integer",
            "default": "1"
        }

    }

    def __init__(self):
        """
        Args:
            _mgt_name: Unique name that represents the microservice
            _mgt_port: Dynamic port of the management API - Used by the Storage layer
            _mgt_address: IP address of the server for the management API - Used by the Storage layer
        Returns:
        Raises:
        """

        # Initialize class attributes
        if not SendingProcess._logger:
            SendingProcess._logger = _LOGGER

        # Configurations retrieved from the Configuration Manager
        self._config = {
            'enable': self._CONFIG_DEFAULT['enable']['default'],
            'duration': int(self._CONFIG_DEFAULT['duration']['default']),
            'source': self._CONFIG_DEFAULT['source']['default'],
            'blockSize': int(self._CONFIG_DEFAULT['blockSize']['default']),
            'memory_buffer_size': int(self._CONFIG_DEFAULT['memory_buffer_size']['default']),
            'sleepInterval': float(self._CONFIG_DEFAULT['sleepInterval']['default']),
            'north': self._CONFIG_DEFAULT['north']['default'],
        }
        self._config_from_manager = ""
        # Plugin handling - loading an empty plugin
        self._module_template = self._NORTH_PATH + "empty." + "empty"
        self._plugin = importlib.import_module(self._module_template)
        self._plugin_info = {
            'name': "",
            'version': "",
            'type': "",
            'interface': "",
            'config': ""
        }
        self._plugin_handle = None
        self._mgt_name = None
        self._mgt_port = None
        self._mgt_address = None
        ''' Parameters for the Storage layer '''
        self._storage_async = None
        self._storage = None
        self._readings = None
        """" Interfaces to the FogLAMP Storage Layer """
        self._audit = None
        """" Used to log operations in the Storage Layer """

        self.input_stream_id = None
        self._log_performance = None
        """ Enable/Disable performance logging, enabled using a command line parameter"""
        self._log_debug_level = None
        """ Defines what and the level of details for logging """

        self._task_fetch_data_run = True
        self._task_send_data_run = True
        """" The specific task will run until the value is True """

        self._task_fetch_data_task_id = None
        self._task_send_data_task_id = None
        """" Used to to managed the fetch/send operations """

        self._task_fetch_data_sem = None
        self._task_send_data_sem = None
        """" Semaphores used for the synchronization of the fetch/send operations """

        self._memory_buffer = [None]
        """" In memory buffer where the data is loaded from the storage layer before to send it to the plugin """

        self._memory_buffer_fetch_idx = 0
        self._memory_buffer_send_idx = 0
        """" Used to to managed the in memory buffer for the fetch/send operations """

        self._event_loop = asyncio.get_event_loop()

    @staticmethod
    def _signal_handler(_signal_num, _stack_frame):
        """ Handles signals to properly terminate the execution

        Args:
        Returns:
        Raises:
        """

        SendingProcess._stop_execution = True

        SendingProcess._logger.info("{func} - signal captured |{signal_num}| ".format(
            func="_signal_handler",
            signal_num=_signal_num))

    def _is_stream_id_valid(self, stream_id):
        """ Checks if the provided stream id  is valid
        Args:
            stream_id: managed stream id
        Returns:
            True/False
        Raises:
        """
        try:
            streams = self._storage.query_tbl('streams', 'id={0}'.format(stream_id))
            rows = streams['rows']
            if len(rows) == 0:
                _message = _MESSAGES_LIST["e000016"].format(str(stream_id))
                raise ValueError(_message)
            elif len(rows) > 1:
                _message = _MESSAGES_LIST["e000014"].format(str(stream_id))
                raise ValueError(_message)
            else:
                if rows[0]['active'] == 't':
                    stream_id_valid = True
                else:
                    _message = _MESSAGES_LIST["i000004"].format(stream_id)
                    SendingProcess._logger.info(_message)
                    stream_id_valid = False
        except Exception as e:
            _message = _MESSAGES_LIST["e000013"].format(str(e))
            SendingProcess._logger.error(_message)
            raise e
        return stream_id_valid

    def _is_north_valid(self):
        """ Checks if the north has adequate characteristics to be used for sending of the data
        Args:
        Returns:
            north_ok: True if the north is a proper one
        Raises:
        """
        north_ok = False
        try:
            if self._plugin_info['type'] == self._PLUGIN_TYPE and \
               self._plugin_info['name'] != "Empty North Plugin":
                north_ok = True
        except Exception:
            _message = _MESSAGES_LIST["e000000"]
            SendingProcess._logger.error(_message)
            raise
        return north_ok

    async def _load_data_into_memory(self, last_object_id):
        """ Identifies the data source requested and call the appropriate handler
        Args:
        Returns:
            data_to_send: a list of elements having each the structure :
                row id     - integer
                asset code - string
                timestamp  - timestamp
                value      - dictionary, like for example {"lux": 53570.172}
        Raises:
            UnknownDataSource
        """
        SendingProcess._logger.debug("{0} ".format("_load_data_into_memory"))
        try:
            if self._config['source'] == self._DATA_SOURCE_READINGS:
                data_to_send = await self._load_data_into_memory_readings(last_object_id)
            elif self._config['source'] == self._DATA_SOURCE_STATISTICS:
                data_to_send = self._load_data_into_memory_statistics(last_object_id)
            elif self._config['source'] == self._DATA_SOURCE_AUDIT:
                data_to_send = self._load_data_into_memory_audit(last_object_id)
            else:
                _message = _MESSAGES_LIST["e000008"]
                SendingProcess._logger.error(_message)
                raise UnknownDataSource
        except Exception:
            _message = _MESSAGES_LIST["e000009"]
            SendingProcess._logger.error(_message)
            raise
        return data_to_send

    async def _load_data_into_memory_readings(self, last_object_id):
        """ Extracts from the DB Layer data related to the readings loading into a memory structure
        Args:
            last_object_id: last value already handled
        Returns:
            raw_data: data extracted from the DB Layer
        Raises:
        """
        SendingProcess._logger.debug("{0} - position {1} ".format("_load_data_into_memory_readings", last_object_id))
        raw_data = None

        converted_data = []
        try:
            # Loads data, +1 as > is needed
            readings = await self._readings.fetch(last_object_id + 1, self._config['blockSize'])

            raw_data = readings['rows']
            converted_data = self._transform_in_memory_data_readings(raw_data)

        except aiohttp.client_exceptions.ClientPayloadError as _ex:

            _message = _MESSAGES_LIST["e000009"].format(str(_ex))
            SendingProcess._logger.warning(_message)

        except Exception as _ex:
            _message = _MESSAGES_LIST["e000009"].format(str(_ex))
            SendingProcess._logger.error(_message)
            raise
        return converted_data

    @staticmethod
    def _transform_in_memory_data_readings(raw_data):
        """ Transforms readings data retrieved form the DB layer to the proper format
        Args:
            raw_data: list of dicts to convert having the structure
                id         : int  - Row id on the storage layer
                asset_code : str  - Asset code
                read_key   : str  - Id of the row
                reading    : dict - Payload
                user_ts    : str  - Timestamp as str
        Returns:
            converted_data: converted data
        Raises:
        """
        converted_data = []

        try:
            for row in raw_data:

                # Converts values to the proper types, for example "180.2" to float 180.2
                payload = row['reading']
                for key in list(payload.keys()):
                    value = payload[key]
                    payload[key] = plugin_common.convert_to_type(value)

                # Adds timezone UTC
                timestamp = apply_date_format(row['user_ts'])

                new_row = {
                    'id': row['id'],
                    'asset_code': row['asset_code'],
                    'read_key': row['read_key'],
                    'reading': payload,
                    'user_ts': timestamp
                }
                converted_data.append(new_row)

        except Exception as e:
            _message = _MESSAGES_LIST["e000022"].format(str(e))
            SendingProcess._logger.error(_message)
            raise e

        return converted_data

    def _load_data_into_memory_statistics(self, last_object_id):
        """ Extracts statistics data from the DB Layer, converts it into the proper format
            loading into a memory structure
        Args:
            last_object_id: last row_id already handled
        Returns:
            converted_data: data extracted from the DB Layer and converted in the proper format
        Raises:
        """
        SendingProcess._logger.debug("{0} - position |{1}| ".format("_load_data_into_memory_statistics", last_object_id))
        raw_data = None
        try:
            payload = payload_builder.PayloadBuilder() \
                .SELECT("id", "key", '{"column": "ts", "timezone": "UTC"}', "value", "history_ts")\
                .WHERE(['id', '>', last_object_id]) \
                .LIMIT(self._config['blockSize']) \
                .ORDER_BY(['id', 'ASC']) \
                .payload()

            statistics_history = self._storage.query_tbl_with_payload('statistics_history', payload)

            raw_data = statistics_history['rows']
            converted_data = self._transform_in_memory_data_statistics(raw_data)
        except Exception:
            _message = _MESSAGES_LIST["e000009"]
            SendingProcess._logger.error(_message)
            raise
        return converted_data

    @staticmethod
    def _transform_in_memory_data_statistics(raw_data):
        """ Transforms statistics data retrieved form the DB layer to the proper format
        Args:
            raw_data: list to convert having the structure
                row id     : int
                asset code : string
                timestamp  : timestamp
                value      : int
        Returns:
            converted_data: converted data
        Raises:
        """
        converted_data = []
        # Extracts only the asset_code column
        # and renames the columns to id, asset_code, user_ts, reading
        try:
            for row in raw_data:
                # Adds timezone UTC
                timestamp = apply_date_format(row['ts'])

                # Removes spaces
                asset_code = row['key'].strip()

                new_row = {
                    'id': row['id'],                    # Row id
                    'asset_code': asset_code,           # Asset code
                    'user_ts': timestamp,               # Timestamp
                    'reading': {'value': row['value']}  # Converts raw data to a Dictionary
                }
                converted_data.append(new_row)
        except Exception as e:
            _message = _MESSAGES_LIST["e000022"].format(str(e))
            SendingProcess._logger.error(_message)
            raise e
        return converted_data

    def _load_data_into_memory_audit(self, last_object_id):
        """ Extracts from the DB Layer data related to the statistics audit into the memory
        #
        Args:
        Returns:
        Raises:
        Todo: TO BE IMPLEMENTED
        """
        SendingProcess._logger.debug("{0} - position {1} ".format("_load_data_into_memory_audit", last_object_id))
        raw_data = None
        try:
            # Temporary code
            if self._module_template != "":
                raw_data = ""
            else:
                raw_data = ""
        except Exception:
            _message = _MESSAGES_LIST["e000000"]
            SendingProcess._logger.error(_message)
            raise
        return raw_data

    def _last_object_id_read(self, stream_id):
        """ Retrieves the starting point for the send operation
        Args:
            stream_id: managed stream id
        Returns:
            last_object_id: starting point for the send operation
        Raises:
        """
        try:
            where = 'id={0}'.format(stream_id)
            streams = self._storage.query_tbl('streams', where)
            rows = streams['rows']
            if len(rows) == 0:
                _message = _MESSAGES_LIST["e000016"].format(str(stream_id))
                raise ValueError(_message)
            elif len(rows) > 1:
                _message = _MESSAGES_LIST["e000014"].format(str(stream_id))
                raise ValueError(_message)
            else:
                last_object_id = rows[0]['last_object']
                SendingProcess._logger.debug("{0} - last_object id |{1}| ".format("_last_object_id_read", last_object_id))
        except Exception:
            _message = _MESSAGES_LIST["e000019"]
            SendingProcess._logger.error(_message)
            raise
        return last_object_id

    async def _last_object_id_update(self, new_last_object_id, stream_id):
        """ Updates reached position
        Args:
            new_last_object_id: Last row id already sent
            stream_id:          Managed stream id
        """
        try:
            SendingProcess._logger.debug("Last position, sent |{0}| ".format(str(new_last_object_id)))
            # TODO : FOGL-623 - avoid the update of the field ts when it will be managed by the DB itself
            #
            payload = payload_builder.PayloadBuilder() \
                .SET(last_object=new_last_object_id, ts='now()') \
                .WHERE(['id', '=', stream_id]) \
                .payload()
            await self._storage_async.update_tbl("streams", payload)

        except Exception as _ex:
            _message = _MESSAGES_LIST["e000020"].format(_ex)
            SendingProcess._logger.error(_message)
            raise

    async def send_data(self, stream_id):
        """ Handles the sending of the data to the destination using the configured plugin
            for a defined amount of time
        Args:
            stream_id:          Managed stream id
        Returns:
        Raises:
        """
        SendingProcess._logger.debug("{0} - start".format("send_data"))

        # Prepares the in memory buffer for the fetch/send operations
        self._memory_buffer = [None for x in range(self._config['memory_buffer_size'])]

        self._task_fetch_data_sem = asyncio.Semaphore(0)
        self._task_send_data_sem = asyncio.Semaphore(0)

        self._task_fetch_data_task_id = asyncio.ensure_future(self._task_fetch_data(stream_id))
        self._task_send_data_task_id = asyncio.ensure_future(self._task_send_data(stream_id))

        self._task_fetch_data_run = True
        self._task_send_data_run = True

        try:
            start_time = time.time()
            elapsed_seconds = 0

            while elapsed_seconds < self._config['duration']:

                # Terminates the execution in case a signal has been received
                if SendingProcess._stop_execution:
                    SendingProcess._logger.info("{func} - signal received, stops the execution".format(
                            func="send_data"))
                    break

                # Context switch to either the fetch or the send operation
                await asyncio.sleep(self._config['sleepInterval'])

                elapsed_seconds = time.time() - start_time
                SendingProcess._logger.debug("{0} - elapsed_seconds {1}".format("send_data", elapsed_seconds))

        except Exception as ex:
            _message = _MESSAGES_LIST["e000021"].format(ex)
            SendingProcess._logger.error(_message)

            await self._audit.failure(self._AUDIT_CODE, {"error - on send_data": _message})

        try:
            # Graceful termination of the tasks
            self._task_fetch_data_run = False
            self._task_send_data_run = False

            # Unblocks the task if it is waiting
            self._task_fetch_data_sem.release()
            self._task_send_data_sem.release()

            await self._task_fetch_data_task_id
            await self._task_send_data_task_id

        except Exception as ex:
            _message = _MESSAGES_LIST["e000029"].format(ex)
            SendingProcess._logger.error(_message)

        SendingProcess._logger.debug("{0} - completed".format("send_data"))

    async def _task_fetch_data(self, stream_id):
        """ Read data from the Storage Layer into a memory structure
        Args:
            stream_id:          Managed stream id
        """

        try:
            last_object_id = self._last_object_id_read(stream_id)
            self._memory_buffer_fetch_idx = 0

            SendingProcess._logger.debug("task {0} - start".format("_task_fetch_data"))

            sleep_time = self.TASK_FETCH_SLEEP
            sleep_num_increments = 1

            while self._task_fetch_data_run:

                slept = False

                if self._memory_buffer_fetch_idx < self._config['memory_buffer_size']:

                    # Checks if there is enough space to load a new block of data
                    if self._memory_buffer[self._memory_buffer_fetch_idx] is None:

                        try:
                            data_to_send = await self._load_data_into_memory(last_object_id)

                        except Exception as ex:
                            _message = _MESSAGES_LIST["e000028"].format(ex)
                            SendingProcess._logger.error(_message)
                            await self._audit.failure(self._AUDIT_CODE, {"error - on _task_fetch_data": _message})

                            data_to_send = False

                            slept = True
                            await asyncio.sleep(sleep_time)

                        if data_to_send:
                            SendingProcess._logger.debug("task {f} - loaded - idx |{idx}|".format(
                                                                        f="fetch_data",
                                                                        idx=self._memory_buffer_fetch_idx))

                            # Handles the JQFilter functionality
                            if self._config_from_manager['applyFilter']["value"].upper() == "TRUE":
                                jqfilter = JQFilter()

                                # Steps needed to proper format the data generated by the JQFilter
                                # to the one expected by the SP
                                data_to_send_2 = jqfilter.transform(data_to_send,
                                                                    self._config_from_manager['filterRule']["value"])
                                data_to_send_3 = json.dumps(data_to_send_2)
                                del data_to_send_2

                                data_to_send_4 = eval(data_to_send_3)
                                del data_to_send_3

                                data_to_send = data_to_send_4[0]
                                del data_to_send_4

                            # Loads the block of data into the in memory buffer
                            self._memory_buffer[self._memory_buffer_fetch_idx] = data_to_send
                            last_position = len(data_to_send) - 1
                            last_object_id = data_to_send[last_position]['id']

                            self._memory_buffer_fetch_idx += 1

                            self._task_fetch_data_sem.release()

                            self.performance_track("task _task_fetch_data")
                        else:
                            # There is no more data to load
                            SendingProcess._logger.debug("task {f} - idle : no more data to load - idx |{idx}| "
                                                         .format(f="fetch_data", idx=self._memory_buffer_fetch_idx))

                            slept = True
                            await asyncio.sleep(sleep_time)

                    else:
                        # There is no more space in the in memory buffer
                        SendingProcess._logger.debug("task {f} - idle : memory buffer full - idx |{idx}| "
                                                     .format(f="fetch_data", idx=self._memory_buffer_fetch_idx))

                        await self._task_send_data_sem.acquire()
                else:
                    self._memory_buffer_fetch_idx = 0

                # Handles the sleep time, it is doubled every time up to a limit
                if slept:
                    sleep_num_increments += 1
                    sleep_time *= 2

                    if sleep_num_increments > self.TASK_SLEEP_MAX_INCREMENTS:
                        sleep_time = self.TASK_FETCH_SLEEP
                        sleep_num_increments = 1

        except Exception as ex:
            _message = _MESSAGES_LIST["e000028"].format(ex)
            SendingProcess._logger.error(_message)

            await self._audit.failure(self._AUDIT_CODE, {"error - on _task_fetch_data": _message})
            raise

        SendingProcess._logger.debug("task {0} - end".format("_task_fetch_data"))

    async def _task_send_data(self, stream_id):
        """ Sends the data from the in memory structure to the destination using the loaded plugin
        Args:
            stream_id: Managed stream id
        """

        data_sent = False
        db_update = False
        update_last_object_id = 0
        tot_num_sent = 0
        update_position_idx = 0

        try:
            self._memory_buffer_send_idx = 0

            SendingProcess._logger.debug("task {0} - start".format("_task_send_data"))

            sleep_time = self.TASK_SEND_SLEEP
            sleep_num_increments = 1

            while self._task_send_data_run:

                slept = False

                if self._memory_buffer_send_idx < self._config['memory_buffer_size']:

                    # Checks if there are data to send
                    if self._memory_buffer[self._memory_buffer_send_idx] is not None:

                        SendingProcess._logger.debug("task {f} - sending - idx |{idx}| ".format(
                                                                    f="send_data",
                                                                    idx=self._memory_buffer_send_idx))

                        try:
                            data_sent, new_last_object_id, num_sent = await self._plugin.plugin_send(
                                self._plugin_handle,
                                self._memory_buffer[self._memory_buffer_send_idx],
                                stream_id)

                        except Exception as ex:
                            _message = _MESSAGES_LIST["e000021"].format(ex)
                            SendingProcess._logger.error(_message)
                            await self._audit.failure(self._AUDIT_CODE, {"error - on _task_send_data": _message})

                            data_sent = False

                            slept = True
                            await asyncio.sleep(sleep_time)

                        if data_sent:
                            db_update = True
                            update_last_object_id = new_last_object_id
                            tot_num_sent = tot_num_sent + num_sent

                            self._memory_buffer[self._memory_buffer_send_idx] = None

                            self._memory_buffer_send_idx += 1

                            self._task_send_data_sem.release()

                            self.performance_track("task _task_send_data")
                    else:
                        # There is no data to send
                        SendingProcess._logger.debug("task {f} - idle : no data to send - idx |{idx}| "
                                                     .format(f="send_data", idx=self._memory_buffer_send_idx))

                        # Updates the position before going to wait for the semaphore
                        if db_update:
                            await self._update_position_reached(stream_id, update_last_object_id, tot_num_sent)
                            update_position_idx = 0
                            tot_num_sent = 0
                            db_update = False

                        await self._task_fetch_data_sem.acquire()

                    # Updates the Storage layer every 'self.UPDATE_POSITION_MAX' interactions
                    if db_update:

                        if update_position_idx >= self.TASK_SEND_UPDATE_POSITION_MAX:

                            SendingProcess._logger.debug("task {f} - update position - idx/max |{idx}/{max}| ".format(
                                            f="send_data",
                                            idx=update_position_idx,
                                            max=self.TASK_SEND_UPDATE_POSITION_MAX))

                            await self._update_position_reached(stream_id, update_last_object_id, tot_num_sent)
                            update_position_idx = 0
                            tot_num_sent = 0
                            db_update = False
                        else:
                            update_position_idx += 1
                else:
                    self._memory_buffer_send_idx = 0

                # Handles the sleep time, it is doubled every time up to a limit
                if slept:
                    sleep_num_increments += 1
                    sleep_time *= 2

                    if sleep_num_increments > self.TASK_SLEEP_MAX_INCREMENTS:
                        sleep_time = self.TASK_SEND_SLEEP
                        sleep_num_increments = 1

            # Checks if the information on the Storage layer needs to be updates
            if db_update:
                await self._update_position_reached(stream_id, update_last_object_id, tot_num_sent)

        except Exception as ex:
            _message = _MESSAGES_LIST["e000021"].format(ex)
            SendingProcess._logger.error(_message)

            if db_update:
                await self._update_position_reached(stream_id, update_last_object_id, tot_num_sent)

            await self._audit.failure(self._AUDIT_CODE, {"error - on _task_send_data": _message})
            raise

        SendingProcess._logger.debug("task {0} - end".format("_task_send_data"))

    async def _update_position_reached(self, stream_id, update_last_object_id, tot_num_sent):
        """ Updates last_object_id, statistics and audit
        Args:
        Returns:
        Raises:
        """

        SendingProcess._logger.debug("{f} - update position - last_object/sent |{last}/{sent}| ".format(
            f="_update_position_reached",
            last=update_last_object_id,
            sent=tot_num_sent))

        await self._last_object_id_update(update_last_object_id, stream_id)

        await self._update_statistics(tot_num_sent, stream_id)

        await self._audit.information(self._AUDIT_CODE, {"sentRows": tot_num_sent})

    async def _update_statistics(self, num_sent, stream_id):
        """ Updates FogLAMP statistics
        Raises :
        """
        try:
            key = 'SENT_' + str(stream_id)
            _stats = await statistics.create_statistics(self._storage_async)

            await _stats.update(key, num_sent)

        except Exception:
            _message = _MESSAGES_LIST["e000010"]
            SendingProcess._logger.error(_message)
            raise

    @staticmethod
    def performance_track(message):
        """ Tracks information for performance measurement
        Args:
        Returns:
        Raises:
        """

        if _log_performance:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            process_memory = usage.ru_maxrss / 1000

            SendingProcess._logger.debug("PERFORMANCE - {0} : memory MB |{1:>8,}|".format(
                                                                message,
                                                                process_memory))

    def _plugin_load(self):
        """ Loads the plugin
        Args:
        Returns:
        Raises:
        """
        module_to_import = self._NORTH_PATH + self._config['north'] + "." + self._config['north']
        try:
            self._plugin = __import__(module_to_import, fromlist=[''])
        except ImportError:
            _message = _MESSAGES_LIST["e000005"].format(module_to_import)
            SendingProcess._logger.error(_message)
            raise

    def _fetch_configuration(self, cat_name=None, cat_desc=None, cat_config=None, cat_keep_original=False):
        """ Retrieves the configuration from the Configuration Manager"""
        SendingProcess._logger.debug("{0} - ".format("_fetch_configuration"))
        cfg_manager = ConfigurationManager(self._storage_async)
        try:
            self._event_loop.run_until_complete(cfg_manager.create_category(cat_name,
                                                                            cat_config,
                                                                            cat_desc,
                                                                            cat_keep_original))
            _config_from_manager = self._event_loop.run_until_complete(cfg_manager.get_category_all_items(cat_name))
            return _config_from_manager
        except Exception:
            _message = _MESSAGES_LIST["e000003"]
            SendingProcess._logger.error(_message)
            raise

    def _retrieve_configuration(self, stream_id, cat_name=None, cat_desc=None, cat_config=None, cat_keep_original=False):
        """ Retrieves the configuration from the Configuration Manager
        Args:
            stream_id: managed stream id
        Returns:
        Raises:
        .. todo::
        """
        SendingProcess._logger.debug("{0} - ".format("_retrieve_configuration"))
        try:
            config_category_name = cat_name if cat_name is not None else self._CONFIG_CATEGORY_NAME + "_" + str(stream_id)
            config_category_desc = cat_desc if cat_desc is not None else self._CONFIG_CATEGORY_DESCRIPTION
            config_category_config = cat_config if cat_config is not None else self._CONFIG_DEFAULT
            if 'stream_id' in config_category_config:
                config_category_config['stream_id']['default'] = str(stream_id)
            _config_from_manager = self._fetch_configuration(config_category_name,
                                                             config_category_desc,
                                                             config_category_config,
                                                             cat_keep_original)
            # Retrieves the configurations and apply the related conversions
            self._config['enable'] = True if _config_from_manager['enable']['value'].upper() == 'TRUE' else False

            self._config['duration'] = int(_config_from_manager['duration']['value'])

            self._config['source'] = _config_from_manager['source']['value']

            self._config['blockSize'] = int(_config_from_manager['blockSize']['value'])

            self._config['memory_buffer_size'] = int(_config_from_manager['memory_buffer_size']['value'])

            self._config['sleepInterval'] = float(_config_from_manager['sleepInterval']['value'])

            self._config['north'] = _config_from_manager['plugin']['value']
            _config_from_manager['_CONFIG_CATEGORY_NAME'] = config_category_name
            self._config_from_manager = _config_from_manager
        except Exception:
            _message = _MESSAGES_LIST["e000003"]
            SendingProcess._logger.error(_message)
            raise

    def _start(self, stream_id):
        """ Setup the correct state for the Sending Process
        Args:
            stream_id: managed stream id
        Returns:
            False = the sending process is disabled
        Raises:
            PluginInitialiseFailed
        """
        exec_sending_process = False
        SendingProcess._logger.debug("{0} - ".format("start"))
        try:
            prg_text = ", for Linux (x86_64)"
            start_message = "" + _MODULE_NAME + "" + prg_text + " " + __copyright__ + " "
            SendingProcess._logger.info("{0}".format(start_message))
            SendingProcess._logger.info(_MESSAGES_LIST["i000001"])
            if self._is_stream_id_valid(stream_id):
                # config from sending process
                self._retrieve_configuration(stream_id, cat_keep_original=True)
                exec_sending_process = self._config['enable']
                if self._config['enable']:
                    self._plugin_load()
                    self._plugin._log_debug_level = self._log_debug_level
                    self._plugin._log_performance = self._log_performance
                    self._plugin_info = self._plugin.plugin_info()
                    SendingProcess._logger.debug("{0} - {1} - {2} ".format("start",
                                                            self._plugin_info['name'],
                                                            self._plugin_info['version']))
                    if self._is_north_valid():
                        try:
                            # config from plugin
                            self._retrieve_configuration(stream_id, cat_config=self._plugin_info['config'], cat_keep_original=True)
                            data = self._config_from_manager
                            data.update({'sending_process_instance': self})
                            self._plugin_handle = self._plugin.plugin_init(data)
                        except Exception as e:
                            _message = _MESSAGES_LIST["e000018"].format(self._plugin_info['name'])
                            SendingProcess._logger.error(_message)
                            raise PluginInitialiseFailed(e)
                    else:
                        exec_sending_process = False
                        _message = _MESSAGES_LIST["e000015"].format(self._plugin_info['type'],
                                                                    self._plugin_info['name'])
                        SendingProcess._logger.warning(_message)
                else:
                    _message = _MESSAGES_LIST["i000003"]
                    SendingProcess._logger.info(_message)
        except Exception as _ex:
            _message = _MESSAGES_LIST["e000004"].format(str(_ex))
            SendingProcess._logger.error(_message)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._audit.failure(self._AUDIT_CODE, {"error - on start": _message}))
            raise
        return exec_sending_process

    def start(self):
        """

        """
        # Command line parameter handling
        global _log_performance
        global _LOGGER

        # Setups signals handlers, to properly handle the termination
        # a) SIGTERM - 15 : kill or system shutdown
        signal.signal(signal.SIGTERM, SendingProcess._signal_handler)

        try:
            self._mgt_name, self._mgt_port, self._mgt_address, self.input_stream_id, self._log_performance, self._log_debug_level = \
                handling_input_parameters()

            _log_performance = self._log_performance

        except Exception as ex:
            message = _MESSAGES_LIST["e000017"].format(str(ex))
            SendingProcess._logger.exception(message)
            sys.exit(1)
        try:
            self._storage_async = StorageClientAsync(self._mgt_address, self._mgt_port)
            self._readings = ReadingsStorageClientAsync(self._mgt_address, self._mgt_port)
            self._storage = StorageClient(self._mgt_address, self._mgt_port)
            self._audit = AuditLogger(self._storage)
        except Exception as ex:
            message = _MESSAGES_LIST["e000023"].format(str(ex))
            SendingProcess._logger.exception(message)
            sys.exit(1)
        else:
            # Reconfigures the logger using the Stream ID to differentiates
            # logging from different processes
            SendingProcess._logger.removeHandler(SendingProcess._logger.handle)
            logger_name = _MODULE_NAME + "_" + str(self.input_stream_id)

            SendingProcess._logger = logger.setup(logger_name, destination=_LOGGER_DESTINATION, level=_LOGGER_LEVEL)

            try:
                # Set the debug level
                if self._log_debug_level == 1:
                    SendingProcess._logger.setLevel(logging.INFO)
                elif self._log_debug_level >= 2:
                    SendingProcess._logger.setLevel(logging.DEBUG)

                # Sets the reconfigured logger
                _LOGGER = SendingProcess._logger

                # Start sending
                if self._start(self.input_stream_id):
                    self._event_loop.run_until_complete(self.send_data(self.input_stream_id))

                # Stop Sending
                self.stop()
                SendingProcess._logger.info(_MESSAGES_LIST["i000002"])
                sys.exit(0)
            except Exception as ex:
                message = _MESSAGES_LIST["e000002"].format(str(ex))
                SendingProcess._logger.exception(message)
                sys.exit(1)

    def stop(self):
        """ Terminates the sending process and the related plugin
        Args:
        Returns:
        Raises:
        """
        try:
            self._plugin.plugin_shutdown(self._plugin_handle)
        except Exception:
            _message = _MESSAGES_LIST["e000007"]
            SendingProcess._logger.error(_message)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self._audit.failure(self._AUDIT_CODE, {"error - on stop": _message}))
            raise


if __name__ == "__main__":

    SendingProcess().start()
