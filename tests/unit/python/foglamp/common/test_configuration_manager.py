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
       

    """ Configuration Manager

    General naming convention:

    category(s)
        category_name - string
        category_description - string
        category_val - dict
            item_name - string (dynamic)
            item_val - dict
                entry_name - string
                entry_val - string

        ----------- 4 fixed entry_name/entry_val pairs ----------------

                description_name - string (fixed - 'description')
                    description_val - string (dynamic)
                type_name - string (fixed - 'type')
                    type_val - string (dynamic - ('boolean', 'integer', 'string', 'IPv4', 'IPv6', 'X509 certificate', 'JSON'))
                default_name - string (fixed - 'default')
                    default_val - string (dynamic)
                value_name - string (fixed - 'value')
                    value_val - string (dynamic)
    """

    # TODO: more category_val type, format, value correctness, type_val validation (negatives also needed - exception checks)
    @pytest.mark.asyncio
    async def test__validate_category_val_valid_config_use_default_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config = { 
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        c_return_value = await c._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert type(c_return_value) is dict 
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test default val"

        # deep copy check to make sure test_config wasn't modified in the method call
        assert test_config is not c_return_value
        assert type(test_config) is dict 
        assert len(test_config) is 1
        test_item_val = test_config.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 3
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test__validate_category_val_valid_config_use_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config = { 
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        c_return_value = await c._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert type(c_return_value) is dict 
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        # deep copy check to make sure test_config wasn't modified in the method call
        assert test_config is not c_return_value
        assert type(test_config) is dict 
        assert len(test_config) is 1
        test_item_val = test_config.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        ConfigurationManagerSingleton._shared_state = {}
    
    @pytest.mark.asyncio
    async def test__validate_category_val_config_without_value_use_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config = { 
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            c_return_value = await c._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'Missing entry_name value for item_name test_item_name' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}


    @pytest.mark.asyncio
    async def test__validate_category_val_config_without_default_notuse_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config = { 
            "test_item_name": {
                "description": "test description val",
                "type": "string",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            c_return_value = await c._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Missing entry_name default for item_name test_item_name' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test__validate_category_val_config_with_default_andvalue_val_notuse_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config = { 
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        with pytest.raises(ValueError) as excinfo:
            c_return_value = await c._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Specifying value_name and value_val for item_name test_item_name is not allowed if desired behavior is to use default_val as value_val' in str(excinfo.value)
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test__merge_category_vals_same_items_different_values(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config_new = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        test_config_storage = {
            "test_item_name": {
                "description": "test description val storage",
                "type": "string",
                "default": "test default val storage",
                "value": "test value val storage"
            },
        }
        c_return_value = await c._merge_category_vals(test_config_new, test_config_storage, keep_original_items=True)
        assert type(c_return_value) is dict
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val storage"
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test__merge_category_vals_no_mutual_items_ignore_original(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config_new = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        test_config_storage = {
            "test_item_name_storage": {
                "description": "test description val storage",
                "type": "string",
                "default": "test default val storage",
                "value": "test value val storage"
            },
        }
        c_return_value = await c._merge_category_vals(test_config_new, test_config_storage, keep_original_items=False)
        assert type(c_return_value) is dict
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage
        ConfigurationManagerSingleton._shared_state = {}

    @pytest.mark.asyncio
    async def test__merge_category_vals_no_mutual_items_include_original(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        test_config_new = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        test_config_storage = {
            "test_item_name_storage": {
                "description": "test description val storage",
                "type": "string",
                "default": "test default val storage",
                "value": "test value val storage"
            },
        }
        c_return_value = await c._merge_category_vals(test_config_new, test_config_storage, keep_original_items=True)
        assert type(c_return_value) is dict
        assert len(c_return_value) is 2
        test_item_val = c_return_value.get("test_item_name")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        test_item_val = c_return_value.get("test_item_name_storage")
        assert type(test_item_val) is dict
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val storage"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val storage"
        assert test_item_val.get("value") is "test value val storage"
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage
        ConfigurationManagerSingleton._shared_state = {}
    
    # async def _read_category_val(self, category_name)
    # TODO: check what to do 
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__read_category_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._read_category_val(category_name)

    # async def _create_new_category(self, category_name, category_val, category_description)
    # TODO: check what to do
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__create_new_category(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._create_new_category(category_name, category_val, category_description)

    # async def _read_all_category_names(self)
    # TODO: check what to do
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__read_all_category_names(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._read_all_category_names()

    # async def _read_item_val(self, category_name, item_name):
    # TODO: check what to do
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__read_item_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._read_item_val(category_name, item_name)

    # async def _read_value_val(self, category_name, item_name):
    # TODO: check what to do
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__read_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._read_value_val(category_name, item_name)

    # async def _update_value_val(self, category_name, item_name, new_value_val):
    # TODO: check what to do
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__update_value_val(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._update_value_val(category_name, item_name, new_value_val)

    # TODO: check what to do
    # async def _update_category(self, category_name, category_val, category_description):
    @pytest.mark.skip(reason="unit tests do not suffice here")
    @pytest.mark.asyncio
    async def test__update_category(self):
        storageClientMock = MagicMock(spec=StorageClient)
        c = ConfigurationManager(storageClientMock)
        return_value = await c._update_category(category_name, category_val, category_description)


"""
    async def get_all_category_names(self):
    async def get_category_all_items(self, category_name):
    async def get_category_item(self, category_name, item_name):
    async def get_category_item_value_entry(self, category_name, item_name):
    async def set_category_item_value_entry(self, category_name, item_name, new_value_entry):
    async def create_category(self, category_name, category_value, category_description='', keep_original_items=False):
"""
