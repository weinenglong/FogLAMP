# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END


import json
from unittest.mock import MagicMock, patch, Mock
import asyncio
from aiohttp import web
import pytest
from foglamp.services.core import routes
from foglamp.services.core import connect
from foglamp.common.storage_client.storage_client import StorageClient
from foglamp.common.configuration_manager import ConfigurationManager


__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"
"""
async def get_categories(request):
async def get_category(request):
async def get_category_item(request):
async def set_configuration_item(request):
async def delete_configuration_item_value(request):
"""
@pytest.allure.feature("unit")
@pytest.allure.story("api", "configuration")
class TestConfiguration:

    @pytest.fixture
    def client(self, loop, test_client):
        app = web.Application(loop=loop)
        # fill the routes table
        routes.setup(app)
        return loop.run_until_complete(test_client(app))


    async def test_get_categories_1_result(self, client):
        async def mock_coro():
            test_config = []
            test_config.append(('key1', 'description1'))
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_all_category_names', return_value=mock_coro()):
                resp = await client.get('/foglamp/category')
                assert 200 == resp.status
                result = await resp.text()
                json_response = json.loads(result)
                assert len(json_response) is 1
                list_item = json_response.get('categories')
                assert len(list_item) is 1
                entries = list_item[0]
                assert len(entries) is 2
                assert entries['key'] == 'key1'
                assert entries['description'] == 'description1'

    async def test_get_categories_2_result(self, client):
        async def mock_coro():
            test_config = []
            test_config.append(('key1', 'description1'))
            test_config.append(('key2', 'description2'))
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_all_category_names', return_value=mock_coro()):
                resp = await client.get('/foglamp/category')
                assert 200 == resp.status
                result = await resp.text()
                json_response = json.loads(result)
                assert len(json_response) is 1
                list_item = json_response.get('categories')
                assert len(list_item) is 2
                entries = list_item[0]
                assert len(entries) is 2
                assert entries['key'] == 'key1'
                assert entries['description'] == 'description1'
                entries = list_item[1]
                assert len(entries) is 2
                assert entries['key'] == 'key2'
                assert entries['description'] == 'description2'

    async def test_get_category_1_result(self, client):
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        async def mock_coro():
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_category_all_items', return_value=mock_coro()):
                resp = await client.get('/foglamp/category/PURGE')
                assert 200 == resp.status
                result = await resp.text()
                json_response = json.loads(result)
                assert len(json_response) is 1
                assert test_config == json_response

    async def test_get_category_item(self, client):
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        async def mock_coro():
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_category_item', return_value=mock_coro()):
                resp = await client.get('/foglamp/category/PURGE/age')
                assert 200 == resp.status
                result = await resp.text()
                json_response = json.loads(result)
                assert len(json_response) is 1
                assert test_config == json_response

    async def test_set_configuration_item(self, client):
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        jsondata = json.dumps({'value':'bla'})
        async def mock_coro():
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_category_item', return_value=mock_coro()):
                with patch.object(ConfigurationManager, 'set_category_item_value_entry', return_value=mock_coro()):
                    resp = await client.put('/foglamp/category/PURGE/age', headers={"Content-type": "application/json"},data=jsondata)
                    assert 200 == resp.status
                    result = await resp.text()
                    json_response = json.loads(result)
                    assert len(json_response) is 1
                    assert test_config == json_response

    async def test_delete_configuration_item_value(self, client):
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        jsondata = json.dumps({'value':'bla'})
        async def mock_coro():
            return test_config
        storage_client_mock = MagicMock(spec=StorageClient)
        configuration_manager_mock = MagicMock(spec=ConfigurationManager)
        with patch.object(connect, 'get_storage', return_value=storage_client_mock):
            with patch.object(ConfigurationManager, 'get_category_item', return_value=mock_coro()):
                with patch.object(ConfigurationManager, 'set_category_item_value_entry', return_value=mock_coro()):
                    resp = await client.delete('/foglamp/category/PURGE/age/value')
                    assert 200 == resp.status
                    result = await resp.text()
                    json_response = json.loads(result)
                    assert len(json_response) is 1
                    assert test_config == json_response

