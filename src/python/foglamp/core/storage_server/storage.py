#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Starts the FogLAMP Storage service"""
import json
import os
import logging
import signal
import sys
import time
import subprocess
import requests
import setproctitle

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
    def run(cls):
        cls._configure_logging()

        # TODO: Think of some method, other than this, to run the external command within this process, otherwise,
        # this creates a defunct parent.
        from subprocess import Popen
        p = Popen(['python3', '-m', 'foglamp.core.storage_server'])
        p.poll()

        # Create storage pid in ~/var/run/storage.pid
        with open(_PID_PATH, 'w') as pid_file:
            pid_file.write(str(p.pid))

    @classmethod
    def start(cls):
        """Starts Storage"""

        from foglamp.core.server_daemon import Daemon
        Daemon._safe_make_dirs(_WORKING_DIR)
        Daemon._safe_make_dirs(os.path.dirname(_PID_PATH))

        pid = cls.get_pid()

        if pid:
            print("Storage is already running in PID {}".format(pid))
        else:
            pid = os.fork()
            if pid == 0:
                cls.run()
            else:
                cls.register_storage()
                # As this is a separate process, its utility ends here.
                sys.exit(0)

    @classmethod
    def register_storage(cls):
        # TODO: First ping storage service to get below "data" response which then will be used to register with Management api
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
            time.sleep(15)

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

        os.remove(_PID_PATH)

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
