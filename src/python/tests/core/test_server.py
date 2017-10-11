"""
The following code tests src/python/foglamp/core/server.py
""" 
import asyncio
import multiprocessing
import socket
import time
import aiohttp.web
from foglamp.core.server import Server

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

SERVER = Server() 

def netcat(hostname: str = '0.0.0.0', port: int = 8082) -> int:
    """
    ping the host and port 
    :return:
        If ping succeed return 0, else return 1
    """
    _socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return _socket.connect_ex((hostname, port)) 

def start_server():
    """Start Server with prepared server""" 
    loop = asyncio.get_event_loop()
    loop.run_until_complete(SERVER.start())

class TestServer:
    """
    The following code tests the _app, start components
    of server.py. It has no need for setup/teardown, since
    there isn't an updating variable
    """  
    def test_make_app(self):
        """
        Test the creation of a new app
        :assert:
            Assert _make_app returns an object of
            aiohttp.web.Application type
        """
        app = SERVER._make_app()
        assert isinstance(app, aiohttp.web.Application) is True

    def test_start_stop_server(self): 
        """
        Test that when called upon server comes up
        :assert:
            Assert server is down/up when expected
        """
        # Server is down
        assert netcat() != 0
        
        # Start Server as background process 
        _process = multiprocessing.Process(target=start_server)
        _process.start()
        time.sleep(5)
        assert netcat() == 0
        
        # Stop Server 
        _process.terminate() 
        time.sleep(5)
        assert netcat() != 0 

