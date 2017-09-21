#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Starts the FogLAMP core service as a daemon

This module can not be called 'daemon' because it conflicts
with the third-party daemon module
"""
import json
import os
import logging
import signal
import sys
import time
import subprocess
import daemon
import requests
from daemon import pidfile

from foglamp import logger
from foglamp.core.service_registry.service_registry import Service

__author__ = "Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_PID_PATH = os.path.expanduser('~/var/run/storage.pid')
_WORKING_DIR = os.path.expanduser('~/var/log')

_WAIT_STOP_SECONDS = 5
"""How many seconds to wait for the core server process to stop"""
_MAX_STOP_RETRY = 5
"""How many times to send TERM signal to core server process when stopping"""


class Storage:
    logging_configured = False
    """Set to true when it's safe to use logging"""

    @staticmethod
    def _safe_make_dirs(path):
        """Creates any missing parent directories

        :param path: The path of the directory to create
        """

        try:
            os.makedirs(path, 0o750)
        except OSError as exception:
            if not os.path.exists(path):
                raise exception

    @classmethod
    def _configure_logging(cls):
        """Alters the root logger to send messages to syslog
           with a filter of WARNING
        """
        if cls.logging_configured:
            return

        logger.setup()
        cls.logging_configured = True

    @classmethod
    def _start_server(cls):
        """Starts the core server"""

        cls._configure_logging()

        from subprocess import call
        call(['python3', '-m', 'foglamp.core.storage_server'])
        # os.system('python3 -m foglamp.core.storage_server')

    @classmethod
    def start(cls):
        """Starts Storage"""

        cls._safe_make_dirs(_WORKING_DIR)
        cls._safe_make_dirs(os.path.dirname(_PID_PATH))

        pid = cls.get_pid()

        if pid:
            print("Storage is already running in PID {}".format(pid))
        else:
            # If it is desirable to output the pid to the console,
            # os.getpid() reports the wrong pid so it's not easy.
            cls.register_storage()
            cls._start_server()

    @classmethod
    def register_storage(cls):
        print("Registering Storage")
        # register the service to test the code
        data = {"type": "Storage", "name": "Storage Services 1", "address": "127.0.0.1", "port": 8084}

        r = requests.post('http://localhost:8083/foglamp/service', data=json.dumps(data), headers={'Content-Type': 'application/json'})
        res = dict(r.json())
        print(res)
        assert 200 == r.status_code
        assert "Service registered successfully" == res["message"]

    @classmethod
    def test_storage(cls):
        while True:
            print("Testing Storage")

    @classmethod
    def stop(cls, pid=None):
        """Stops Storage if it is running

        Args:
            pid: Optional process id to stop. If not specified, the pidfile is read.

        Raises TimeoutError:
            Unable to stop Storage. Wait and try again.
        """

        if not pid:
            pid = cls.get_pid()

        if not pid:
            print("Storage is not running")
            return

        stopped = False

        try:
            for _ in range(_MAX_STOP_RETRY):
                os.kill(pid, signal.SIGTERM)

                for _ in range(_WAIT_STOP_SECONDS):  # Ignore the warning
                    os.kill(pid, 0)
                    time.sleep(1)
        except OSError:
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Storage")

        print("Storage stopped")

    @classmethod
    def restart(cls):
        """Restarts Storage"""

        pid = cls.get_pid()
        if pid:
            cls.stop(pid)

        cls.start()

    @staticmethod
    def get_pid():
        """Returns Storage's process id or None if Storage is not running"""

        try:
            with open(_PID_PATH, 'r') as pid_file:
                pid = int(pid_file.read().strip())
        except (IOError, ValueError):
            return None

        # Delete the pid file if the process isn't alive
        # there is an unavoidable race condition here if another
        # process is stopping or starting the daemon
        try:
            os.kill(pid, 0)
        except OSError:
            os.remove(_PID_PATH)
            pid = None

        return pid

    @classmethod
    def status(cls):
        """Outputs the status of the Storage process"""
        pid = cls.get_pid()

        if pid:
            print("Storage is running in PID {}".format(pid))
        else:
            print("Storage is not running")
            sys.exit(2)

if __name__ == '__main__':
    Storage.test_storage()
