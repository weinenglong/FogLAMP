# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""Core server module"""
import os
import signal
import asyncio
import setproctitle
import sys

import time
from aiohttp import web
from multiprocessing import Process
from foglamp import logger
from foglamp.core import routes
from foglamp.core import routes_core
from foglamp.core import middleware
from foglamp.core.scheduler import Scheduler

__author__ = "Praveen Garg, Terris Linenbach, Amarendra K Sinha"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)  # logging.Logger
_MANAGEMENT_PID_PATH = os.path.expanduser('~/var/run/management.pid')
_WORKING_DIR = os.path.expanduser('~/var/log')

_WAIT_STOP_SECONDS = 5
"""How many seconds to wait for the core server process to stop"""
_MAX_STOP_RETRY = 5
"""How many times to send TERM signal to core server process when stopping"""


class Server:
    """FOGLamp core server. Starts the FogLAMP scheduler and the FogLAMP REST server."""

    """Class attributes"""
    scheduler = None
    """ foglamp.core.Scheduler """

    @staticmethod
    def _make_app():
        """Creates the REST server

        :rtype: web.Application
        """
        app = web.Application(middlewares=[middleware.error_middleware])
        routes.setup(app)
        return app

    @classmethod
    async def _start_scheduler(cls):
        """Starts the scheduler"""
        cls.scheduler = Scheduler()
        await cls.scheduler.start()

    @classmethod
    def _start(cls):
        """Starts the server"""
        loop = asyncio.get_event_loop()

        # Register signal handlers
        # Registering SIGTERM creates an error at shutdown. See
        # https://github.com/python/asyncio/issues/396
        for signal_name in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                signal_name,
                lambda: asyncio.ensure_future(cls._stop(loop)))

        # The scheduler must start first because the REST API interacts with it
        loop.run_until_complete(asyncio.ensure_future(cls._start_scheduler()))

        # https://aiohttp.readthedocs.io/en/stable/_modules/aiohttp/web.html#run_app
        web.run_app(cls._make_app(), host='0.0.0.0', port=8082, handle_signals=False)

    @staticmethod
    def _make_core():
        """Creates the REST server

        :rtype: web.Application
        """
        core = web.Application(middlewares=[middleware.error_middleware])
        routes_core.setup(core)
        return core

    @classmethod
    def _run_management_api(cls):
        web.run_app(cls._make_core(), host='0.0.0.0', port=8083)

    @classmethod
    def start(cls):
        try:
            try:
                setproctitle.setproctitle('management')
                m = Process(target=cls._run_management_api, name='management')
                m.start()

                # Create storage pid in ~/var/run/storage.pid
                with open(_MANAGEMENT_PID_PATH, 'w') as pid_file:
                    pid_file.write(str(m.pid))

                # Allow Management core api to start
                time.sleep(3)
            except OSError as e:
                raise("%s [%d]".format(e.strerror, e.errno))

            try:
                setproctitle.setproctitle('storage')
                from foglamp.core.storage_server.storage import Storage
                s = Process(target=Storage.start, name='storage')
                s.start()
            except OSError as e:
                raise("%s [%d]".format(e.strerror, e.errno))

            try:
                setproctitle.setproctitle('foglamp')
                cls._start()
            except OSError as e:
                raise("%s [%d]".format(e.strerror, e.errno))

        except Exception as e:
            sys.stderr.write(format(str(e)) + "\n");
            sys.exit(1)

    @staticmethod
    def get_management_pid():
        """Returns FogLAMP's process id or None if FogLAMP is not running"""

        try:
            with open(_MANAGEMENT_PID_PATH, 'r') as pid_file:
                pid = int(pid_file.read().strip())
        except (IOError, ValueError):
            return None

        # Delete the pid file if the process isn't alive
        # there is an unavoidable race condition here if another
        # process is stopping or starting the daemon
        try:
            os.kill(pid, 0)
        except OSError:
            os.remove(_MANAGEMENT_PID_PATH)
            pid = None

        return pid

    @classmethod
    def stop_management(cls, pid=None):
        """Stops Storage if it is running

        Args:
            pid: Optional process id to stop. If not specified, the pidfile is read.

        Raises TimeoutError:
            Unable to stop Storage. Wait and try again.
        """
        if not pid:
            pid = cls.get_management_pid()

        if not pid:
            print("Management API is not running")
            return

        stopped = False

        try:
            for _ in range(_MAX_STOP_RETRY):
                os.kill(pid, signal.SIGTERM)

                for _ in range(_WAIT_STOP_SECONDS):  # Ignore the warning
                    os.kill(pid, 0)
                    time.sleep(1)
                    os.remove(_MANAGEMENT_PID_PATH)
        except OSError:
            stopped = True

        if not stopped:
            raise TimeoutError("Unable to stop Management API")

        print("Management API stopped")

    @classmethod
    async def _stop(cls, loop):
        """Attempts to stop the server

        If the scheduler stops successfully, the event loop is
        stopped.
        """
        if cls.scheduler:
            try:
                await cls.scheduler.stop()
                cls.scheduler = None
            except TimeoutError:
                _LOGGER.exception('Unable to stop the scheduler')
                return

        # Cancel asyncio tasks
        for task in asyncio.Task.all_tasks():
            task.cancel()

        loop.stop()

        cls.stop_management()

        from foglamp.core.storage_server.storage import Storage
        Storage.stop()

