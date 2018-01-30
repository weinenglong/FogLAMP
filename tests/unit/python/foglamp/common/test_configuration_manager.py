"""
The following tests the configuration manager component For the most part,
the code uses the boolean type for testing due to simplicity; but contains
tests to verify which data_types are supported and which are not.
"""

import asyncio
import pytest
from foglamp.common.configuration_manager import ConfigurationManager
from foglamp.common.configuration_manager import ConfigurationManagerSingleton
from foglamp.common.storage_client.storage_client import StorageClient
from unittest.mock import MagicMock
from unittest.mock import patch

__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


class TestConfigurationManager():
    def test_constructor_no_storage_client_defined_no_storage_client_passed(self):
        with pytest.raises(TypeError) as excinfo:
            ConfigurationManager()
        assert 'Must be a valid Storage object' in str(excinfo.value)

    def test_constructor_no_storage_client_defined_storage_client_passed(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        assert hasattr(c, '_storage')
        assert c._storage is storageClientMock
        assert hasattr(c, '_registered_interests')
        ConfigurationManagerSingleton._shared_state = {}

    def test_constructor_storage_client_defined_storage_client_passed(self):
        storageClientMock = MagicMock(spec=StorageClient)
        storageClientMock2 = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        c = ConfigurationManager(storageClientMock2)
        assert hasattr(c, '_storage')
        assert c._storage is storageClientMock
        assert hasattr(c, '_registered_interests')
        ConfigurationManagerSingleton._shared_state = {}

    def test_constructor_storage_client_defined_no_storage_client_passed(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        c = ConfigurationManager()
        assert hasattr(c, '_storage')
        assert c._storage is storageClientMock
        assert hasattr(c, '_registered_interests')
        ConfigurationManagerSingleton._shared_state = {}

    def test_register_interest_no_category_name(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        with pytest.raises(ValueError) as excinfo:
            c.register_interest(None,'callback')
        assert 'Failed to register interest. category_name cannot be None' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    def test_register_interest_no_callback(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        with pytest.raises(ValueError) as excinfo:
            c.register_interest('name',None)
        assert 'Failed to register interest. callback cannot be None' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    def test_register_interest(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        c.register_interest('name','callback')
        assert 'callback' in c._registered_interests['name']
        ConfigurationManagerSingleton._shared_state = {}

    def test_unregister_interest_no_category_name(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        with pytest.raises(ValueError) as excinfo:
            c.unregister_interest(None,'callback')
        assert 'Failed to unregister interest. category_name cannot be None' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    def test_unregister_interest_no_callback(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        with pytest.raises(ValueError) as excinfo:
            c.unregister_interest('name',None)
        assert 'Failed to unregister interest. callback cannot be None' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    def test_unregister_interest(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        c.register_interest('name','callback')
        c.unregister_interest('name','callback')
        assert len(c._registered_interests) is 0
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test_run_callback(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        c.register_interest('name','configuration_manager_callback')
        await c._run_callbacks('name')
        ConfigurationManagerSingleton._shared_state = {}
       
"""
    async def _merge_category_vals(self, category_val_new, category_val_storage, keep_original_items):
    async def _validate_category_val(self, category_val, set_value_val_from_default_val=True):
    async def _create_new_category(self, category_name, category_val, category_description):
    async def _read_all_category_names(self):
    async def _read_category_val(self, category_name):
    async def _read_item_val(self, category_name, item_name):
    async def _read_value_val(self, category_name, item_name):
    async def _update_value_val(self, category_name, item_name, new_value_val):
    async def _update_category(self, category_name, category_val, category_description):
    async def get_all_category_names(self):
    async def get_category_all_items(self, category_name):
    async def get_category_item(self, category_name, item_name):
    async def get_category_item_value_entry(self, category_name, item_name):
    async def set_category_item_value_entry(self, category_name, item_name, new_value_entry):
    async def create_category(self, category_name, category_value, category_description='', keep_original_items=False):
"""
