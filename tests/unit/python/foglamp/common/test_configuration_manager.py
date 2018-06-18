# -*- coding: utf-8 -*-

import asyncio
import json
from unittest.mock import MagicMock, patch, call
import pytest

from foglamp.common.configuration_manager import ConfigurationManager, ConfigurationManagerSingleton, _valid_type_strings, _logger
from foglamp.common.storage_client.payload_builder import PayloadBuilder
from foglamp.common.storage_client.storage_client import StorageClientAsync
from foglamp.common.audit_logger import AuditLogger

__author__ = "Ashwin Gopalakrishnan"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


@pytest.allure.feature("unit")
@pytest.allure.story("common", "configuration_manager")
class TestConfigurationManager:
    @pytest.fixture()
    def reset_singleton(self):
        # executed before each test
        ConfigurationManagerSingleton._shared_state = {}
        yield
        ConfigurationManagerSingleton._shared_state = {}

    def test_constructor_no_storage_client_defined_no_storage_client_passed(
            self, reset_singleton):
        # first time initializing ConfigurationManager without storage client
        # produces error
        with pytest.raises(TypeError) as excinfo:
            ConfigurationManager()
        assert 'Must be a valid Storage object' in str(excinfo.value)

    def test_constructor_no_storage_client_defined_storage_client_passed(
            self, reset_singleton):
        # first time initializing ConfigurationManager with storage client
        # works
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        assert hasattr(c_mgr, '_storage')
        assert isinstance(c_mgr._storage, StorageClientAsync)
        assert hasattr(c_mgr, '_registered_interests')

    def test_constructor_storage_client_defined_storage_client_passed(
            self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        # second time initializing ConfigurationManager with new storage client
        # works
        storage_client_mock2 = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr2 = ConfigurationManager(storage_client_mock2)
        assert hasattr(c_mgr2, '_storage')
        # ignore new storage client
        assert isinstance(c_mgr2._storage, StorageClientAsync)
        assert hasattr(c_mgr2, '_registered_interests')

    def test_constructor_storage_client_defined_no_storage_client_passed(
            self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        # second time initializing ConfigurationManager without storage client
        # works
        c_mgr2 = ConfigurationManager()
        assert hasattr(c_mgr2, '_storage')
        assert isinstance(c_mgr2._storage, StorageClientAsync)
        assert hasattr(c_mgr2, '_registered_interests')
        assert 0 == len(c_mgr._registered_interests)

    def test_register_interest_no_category_name(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with pytest.raises(ValueError) as excinfo:
            c_mgr.register_interest(None, 'callback')
        assert 'Failed to register interest. category_name cannot be None' in str(
            excinfo.value)

    def test_register_interest_no_callback(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with pytest.raises(ValueError) as excinfo:
            c_mgr.register_interest('name', None)
        assert 'Failed to register interest. callback cannot be None' in str(
            excinfo.value)

    def test_register_interest(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest('name', 'callback')
        assert 'callback' in c_mgr._registered_interests['name']
        assert 1 == len(c_mgr._registered_interests)

    def test_unregister_interest_no_category_name(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with pytest.raises(ValueError) as excinfo:
            c_mgr.unregister_interest(None, 'callback')
        assert 'Failed to unregister interest. category_name cannot be None' in str(
            excinfo.value)

    def test_unregister_interest_no_callback(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with pytest.raises(ValueError) as excinfo:
            c_mgr.unregister_interest('name', None)
        assert 'Failed to unregister interest. callback cannot be None' in str(
            excinfo.value)

    def test_unregister_interest(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest('name', 'callback')
        assert 1 == len(c_mgr._registered_interests)
        c_mgr.unregister_interest('name', 'callback')
        assert len(c_mgr._registered_interests) is 0

    @pytest.mark.asyncio
    async def test__run_callbacks(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest('name', 'configuration_manager_callback')
        await c_mgr._run_callbacks('name')

    @pytest.mark.asyncio
    async def test__run_callbacks_invalid_module(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest('name', 'invalid')
        with patch.object(_logger, "error") as log_error:
            with pytest.raises(Exception) as excinfo:
                await c_mgr._run_callbacks('name')
            assert excinfo.type is ImportError
            assert "No module named 'invalid'" == str(excinfo.value)
        assert 1 == log_error.call_count
        log_error.assert_called_once_with('Unable to import callback module %s for category_name %s', 'invalid', 'name', exc_info=True)

    @pytest.mark.asyncio
    async def test__run_callbacks_norun(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest('name', 'configuration_manager_callback_norun')
        with patch.object(_logger, "error") as log_error:
            with pytest.raises(Exception) as excinfo:
                await c_mgr._run_callbacks('name')
            assert excinfo.type is AttributeError
            assert 'Callback module configuration_manager_callback_norun does not have method run' in str(
                excinfo.value)
        assert 1 == log_error.call_count
        log_error.assert_called_once_with('Callback module %s does not have method run', 'configuration_manager_callback_norun', exc_info=True)

    @pytest.mark.asyncio
    async def test__run_callbacks_nonasync(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        c_mgr.register_interest(
            'name', 'configuration_manager_callback_nonasync')
        with patch.object(_logger, "error") as log_error:
            with pytest.raises(Exception) as excinfo:
                await c_mgr._run_callbacks('name')
            assert excinfo.type is AttributeError
            assert 'Callback module configuration_manager_callback_nonasync run method must be a coroutine function' in str(
                excinfo.value)
        assert 1 == log_error.call_count
        log_error.assert_called_once_with('Callback module %s run method must be a coroutine function', 'configuration_manager_callback_nonasync', exc_info=True)

    @pytest.mark.asyncio
    async def test__validate_category_val_valid_config_use_default_val(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val"
            },
        }
        c_return_value = await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert isinstance(c_return_value, dict)
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test default val"

        # deep copy check to make sure test_config wasn't modified in the
        # method call
        assert test_config is not c_return_value
        assert isinstance(test_config, dict)
        assert len(test_config) is 1
        test_item_val = test_config.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 3
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"

    @pytest.mark.asyncio
    async def test__validate_category_val_valid_config_use_value_val(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        c_return_value = await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert isinstance(c_return_value, dict)
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        # deep copy check to make sure test_config wasn't modified in the
        # method call
        assert test_config is not c_return_value
        assert isinstance(test_config, dict)
        assert len(test_config) is 1
        test_item_val = test_config.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"

    @pytest.mark.asyncio
    async def test__validate_category_val_config_without_value_use_value_val(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'Missing entry_name value for item_name test_item_name' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_not_dictionary(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = ()
        with pytest.raises(TypeError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'category_val must be a dictionary' in str(excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_item_name_not_string(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            5: {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
            },
        }
        with pytest.raises(TypeError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'item_name must be a string' in str(excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_item_value_not_dictionary(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": ()
        }
        with pytest.raises(TypeError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'item_value must be a dict for item_name test_item_name' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_entry_name_not_string(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                5: "bla"
            },
        }
        with pytest.raises(TypeError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'entry_name must be a string for item_name test_item_name' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_entry_val_not_string(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "something": 5
            },
        }
        with pytest.raises(TypeError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=False)
        assert 'entry_val must be a string for item_name test_item_name and entry_name something' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_unrecognized_entry_name(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "unrecognized": "unexpected",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Unrecognized entry_name unrecognized for item_name test_item_name' in str(
            excinfo.value)

    @pytest.mark.parametrize("test_input", _valid_type_strings)
    @pytest.mark.asyncio
    async def test__validate_category_val_valid_type(self, reset_singleton, test_input):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": test_input,
                "default": "test default val",
            },
        }
        c_return_value = await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert c_return_value["test_item_name"]["type"] == test_input

    @pytest.mark.asyncio
    async def test__validate_category_val_invalid_type(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "blablabla",
                "default": "test default val",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Invalid entry_val for entry_name "type" for item_name test_item_name. valid: {}'.format(
            _valid_type_strings) in str(excinfo.value)

    @pytest.mark.parametrize("test_input", ["type", "description", "default"])
    @pytest.mark.asyncio
    async def test__validate_category_val_missing_entry(self, reset_singleton, test_input):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
            },
        }
        del test_config['test_item_name'][test_input]
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Missing entry_name {} for item_name {}'.format(
            test_input, "test_item_name") in str(excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_without_default_notuse_value_val(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
            },
        }
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Missing entry_name default for item_name test_item_name' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__validate_category_val_config_with_default_andvalue_val_notuse_value_val(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        test_config = {
            "test_item_name": {
                "description": "test description val",
                "type": "string",
                "default": "test default val",
                "value": "test value val"
            },
        }
        with pytest.raises(ValueError) as excinfo:
            await c_mgr._validate_category_val(category_val=test_config, set_value_val_from_default_val=True)
        assert 'Specifying value_name and value_val for item_name test_item_name is not allowed if desired behavior is to use default_val as value_val' in str(
            excinfo.value)

    @pytest.mark.asyncio
    async def test__merge_category_vals_same_items_different_values(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
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
        c_return_value = await c_mgr._merge_category_vals(test_config_new, test_config_storage, keep_original_items=True)
        assert isinstance(c_return_value, dict)
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        # use value val from storage
        assert test_item_val.get("value") is "test value val storage"
        # return new dictionary, do not modify parameters passed in
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage

    @pytest.mark.asyncio
    async def test__merge_category_vals_no_mutual_items_ignore_original(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
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
        c_return_value = await c_mgr._merge_category_vals(test_config_new, test_config_storage, keep_original_items=False)
        assert isinstance(c_return_value, dict)
        # ignore "test_item_name_storage" and include "test_item_name"
        assert len(c_return_value) is 1
        test_item_val = c_return_value.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage

    @pytest.mark.asyncio
    async def test__merge_category_vals_no_mutual_items_include_original(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
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
        c_return_value = await c_mgr._merge_category_vals(test_config_new, test_config_storage, keep_original_items=True)
        assert isinstance(c_return_value, dict)
        # include "test_item_name_storage" and "test_item_name"
        assert len(c_return_value) is 2
        test_item_val = c_return_value.get("test_item_name")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get("description") is "test description val"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val"
        assert test_item_val.get("value") is "test value val"
        test_item_val = c_return_value.get("test_item_name_storage")
        assert isinstance(test_item_val, dict)
        assert len(test_item_val) is 4
        assert test_item_val.get(
            "description") is "test description val storage"
        assert test_item_val.get("type") is "string"
        assert test_item_val.get("default") is "test default val storage"
        assert test_item_val.get("value") is "test value val storage"
        assert test_config_new is not c_return_value
        assert test_config_storage is not c_return_value
        assert test_config_new is not test_config_storage

    @pytest.mark.asyncio
    async def test_create_category_good_newval_bad_storageval_good_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_validate_category_val', side_effect=[async_mock({}), Exception()]) as valpatch:
                with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock({})) as readpatch:
                    with patch.object(ConfigurationManager, '_merge_category_vals') as mergepatch:
                        with patch.object(ConfigurationManager, '_run_callbacks', return_value=async_mock(None)) as callbackpatch:
                            with patch.object(ConfigurationManager, '_update_category', return_value=async_mock(None)) as updatepatch:
                                await c_mgr.create_category('catname', 'catvalue', 'catdesc')
                            updatepatch.assert_called_once_with('catname', {}, 'catdesc')
                        callbackpatch.assert_called_once_with('catname')
                    mergepatch.assert_not_called()
                readpatch.assert_called_once_with('catname')
            valpatch.assert_has_calls([call('catvalue', True), call({}, False)])
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('category_value for category_name %s from storage is corrupted; using category_value without merge', 'catname')

    @pytest.mark.asyncio
    async def test_create_category_good_newval_bad_storageval_bad_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_validate_category_val', side_effect=[async_mock({}), Exception()]) as valpatch:
                with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock({})) as readpatch:
                    with patch.object(ConfigurationManager, '_merge_category_vals') as mergepatch:
                        with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                            with patch.object(ConfigurationManager, '_update_category', side_effect=Exception()) as updatepatch:
                                with pytest.raises(Exception):
                                    await c_mgr.create_category('catname', 'catvalue', 'catdesc')
                            updatepatch.assert_called_once_with('catname', {}, 'catdesc')
                        callbackpatch.assert_not_called()
                    mergepatch.assert_not_called()
                readpatch.assert_called_once_with('catname')
            valpatch.assert_has_calls([call('catvalue', True), call({}, False)])
        assert 2 == log_exc.call_count
        calls = [call('category_value for category_name %s from storage is corrupted; using category_value without merge', 'catname'),
                 call('Unable to create new category based on category_name %s and category_description %s and category_json_schema %s', 'catname', 'catdesc')]
        assert log_exc.has_calls(calls, any_order=True)

    # (merged_value)
    @pytest.mark.asyncio
    async def test_create_category_good_newval_good_storageval_nochange(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_validate_category_val', side_effect=[async_mock({}), async_mock({})]) as valpatch:
            with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock({})) as readpatch:
                with patch.object(ConfigurationManager, '_merge_category_vals', return_value=async_mock({})) as mergepatch:
                    with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                        with patch.object(ConfigurationManager, '_update_category') as updatepatch:
                            await c_mgr.create_category('catname', 'catvalue', 'catdesc')
                        updatepatch.assert_not_called()
                    callbackpatch.assert_not_called()
                mergepatch.assert_called_once_with({}, {}, False)
            readpatch.assert_called_once_with('catname')
        valpatch.assert_has_calls([call('catvalue', True), call({}, False)])

    @pytest.mark.asyncio
    async def test_create_category_good_newval_good_storageval_good_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_validate_category_val', side_effect=[async_mock({}), async_mock({})]) as valpatch:
            with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock({})) as readpatch:
                with patch.object(ConfigurationManager, '_merge_category_vals', return_value=async_mock({'bla': 'bla'})) as mergepatch:
                    with patch.object(ConfigurationManager, '_run_callbacks', return_value=async_mock(None)) as callbackpatch:
                        with patch.object(ConfigurationManager, '_update_category', return_value=async_mock(None)) as updatepatch:
                            await c_mgr.create_category('catname', 'catvalue', 'catdesc')
                        updatepatch.assert_called_once_with('catname', {'bla': 'bla'}, 'catdesc')
                    callbackpatch.assert_called_once_with('catname')
                mergepatch.assert_called_once_with({}, {}, False)
            readpatch.assert_called_once_with('catname')
        valpatch.assert_has_calls([call('catvalue', True), call({}, False)])

    @pytest.mark.asyncio
    async def test_create_category_good_newval_good_storageval_bad_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_validate_category_val', side_effect=[async_mock({}), async_mock({})]) as valpatch:
                with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock({})) as readpatch:
                    with patch.object(ConfigurationManager, '_merge_category_vals', return_value=async_mock({'bla': 'bla'})) as mergepatch:
                        with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                            with patch.object(ConfigurationManager, '_update_category', side_effect=Exception()) as updatepatch:
                                with pytest.raises(Exception):
                                    await c_mgr.create_category('catname', 'catvalue', 'catdesc')
                                updatepatch.assert_called_once_with('catname', {'bla': 'bla'}, 'catdesc')
                            callbackpatch.assert_not_called()
                        mergepatch.assert_called_once_with({}, {}, False)
                    readpatch.assert_called_once_with('catname')
            valpatch.assert_has_calls([call('catvalue', True), call({}, False)])
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to create new category based on category_name %s and category_description %s '
                                        'and category_json_schema %s', 'catname', 'catdesc', {'bla': 'bla'})

    @pytest.mark.asyncio
    async def test_create_category_good_newval_no_storageval_good_create(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_validate_category_val', return_value=async_mock(None)) as valpatch:
            with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock(None)) as readpatch:
                with patch.object(ConfigurationManager, '_create_new_category', return_value=async_mock(None)) as createpatch:
                    with patch.object(ConfigurationManager, '_run_callbacks', return_value=async_mock(None)) as callbackpatch:
                        await c_mgr.create_category('catname', 'catvalue', "catdesc")
                    callbackpatch.assert_called_once_with('catname')
                createpatch.assert_called_once_with('catname', None, 'catdesc')
            readpatch.assert_called_once_with('catname')
        valpatch.assert_called_once_with('catvalue', True)

    @pytest.mark.asyncio
    async def test_create_category_good_newval_no_storageval_bad_create(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_validate_category_val', return_value=async_mock(None)) as valpatch:
                with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock(None)) as readpatch:
                    with patch.object(ConfigurationManager, '_create_new_category', side_effect=Exception()) as createpatch:
                        with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                            with pytest.raises(Exception):
                                await c_mgr.create_category('catname', 'catvalue', "catdesc")
                        callbackpatch.assert_not_called()
                    createpatch.assert_called_once_with('catname', None, 'catdesc')
                readpatch.assert_called_once_with('catname')
            valpatch.assert_called_once_with('catvalue', True)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to create new category based on category_name %s and category_description %s and category_json_schema %s', 'catname', 'catdesc', None)

    @pytest.mark.asyncio
    async def test_create_category_bad_newval(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_validate_category_val', side_effect=Exception()) as valpatch:
                with patch.object(ConfigurationManager, '_read_category_val') as readpatch:
                    with patch.object(ConfigurationManager, '_create_new_category') as createpatch:
                        with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                            with pytest.raises(Exception):
                                await c_mgr.create_category('catname', 'catvalue', "catdesc")
                        callbackpatch.assert_not_called()
                    createpatch.assert_not_called()
                readpatch.assert_not_called()
            valpatch.assert_called_once_with('catvalue', True)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to create new category based on category_name %s and category_description %s and category_json_schema %s', 'catname', 'catdesc', '')

    @pytest.mark.asyncio
    async def test_set_category_item_value_entry_good_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        category_name = 'catname'
        item_name = 'itemname'
        new_value_entry = 'newvalentry'
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_read_value_val', return_value=async_mock({})) as readpatch:
            with patch.object(ConfigurationManager, '_update_value_val', return_value=async_mock(None)) as updatepatch:
                with patch.object(ConfigurationManager, '_run_callbacks', return_value=async_mock(None)) as callbackpatch:
                    await c_mgr.set_category_item_value_entry(category_name, item_name, new_value_entry)
                callbackpatch.assert_called_once_with(category_name)
            updatepatch.assert_called_once_with(category_name, item_name, new_value_entry)
        readpatch.assert_called_once_with(category_name, item_name)

    @pytest.mark.asyncio
    async def test_set_category_item_value_entry_bad_update(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        category_name = 'catname'
        item_name = 'itemname'
        new_value_entry = 'newvalentry'
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_value_val', return_value=async_mock({})) as readpatch:
                with patch.object(ConfigurationManager, '_update_value_val', side_effect=Exception()) as updatepatch:
                    with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                        with pytest.raises(Exception):
                            await c_mgr.set_category_item_value_entry(category_name, item_name, new_value_entry)
                    callbackpatch.assert_not_called()
                updatepatch.assert_called_once_with(category_name, item_name, new_value_entry)
            readpatch.assert_called_once_with(category_name, item_name)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to set item value entry based on category_name %s and item_name %s and value_item_entry %s', 'catname', 'itemname', 'newvalentry')

    @pytest.mark.asyncio
    async def test_set_category_item_value_entry_bad_storage(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)

        category_name = 'catname'
        item_name = 'itemname'
        new_value_entry = 'newvalentry'
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_value_val', return_value=async_mock(None)) as readpatch:
                with patch.object(ConfigurationManager, '_update_value_val') as updatepatch:
                    with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                        with pytest.raises(ValueError) as excinfo:
                            await c_mgr.set_category_item_value_entry(category_name, item_name, new_value_entry)
                        assert 'No detail found for the category_name: {} and item_name: {}'.format(category_name, item_name) in str(excinfo.value)
                    callbackpatch.assert_not_called()
                updatepatch.assert_not_called()
            readpatch.assert_called_once_with(category_name, item_name)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to set item value entry based on category_name %s and item_name %s and value_item_entry %s', 'catname', 'itemname', 'newvalentry')

    @pytest.mark.asyncio
    async def test_set_category_item_value_entry_no_change(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)

        category_name = 'catname'
        item_name = 'itemname'
        new_value_entry = 'newvalentry'

        with patch.object(ConfigurationManager, '_read_value_val', return_value=async_mock(new_value_entry)) as readpatch:
            with patch.object(ConfigurationManager, '_update_value_val') as updatepatch:
                with patch.object(ConfigurationManager, '_run_callbacks') as callbackpatch:
                    await c_mgr.set_category_item_value_entry(category_name, item_name, new_value_entry)
                callbackpatch.assert_not_called()
            updatepatch.assert_not_called()
        readpatch.assert_called_once_with(category_name, item_name)

    @pytest.mark.asyncio
    async def test_get_all_category_names_good(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_read_all_category_names', return_value=async_mock('bla')) as readpatch:
            ret_val = await c_mgr.get_all_category_names()
            assert 'bla' == ret_val
        readpatch.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_get_all_category_names_bad(self, reset_singleton):
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_all_category_names', side_effect=Exception()) as readpatch:
                with pytest.raises(Exception):
                    await c_mgr.get_all_category_names()
            readpatch.assert_called_once_with()
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to read all category names')

    @pytest.mark.asyncio
    async def test_get_category_all_items_good(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        category_name = 'catname'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_read_category_val', return_value=async_mock('bla')) as readpatch:
            ret_val = await c_mgr.get_category_all_items(category_name)
            assert 'bla' == ret_val
        readpatch.assert_called_once_with(category_name)

    @pytest.mark.asyncio
    async def test_get_category_all_items_bad(self, reset_singleton):
        category_name = 'catname'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_category_val', side_effect=Exception()) as readpatch:
                with pytest.raises(Exception):
                    await c_mgr.get_category_all_items(category_name)
            readpatch.assert_called_once_with(category_name)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to get all category names based on category_name %s', 'catname')

    @pytest.mark.asyncio
    async def test_get_category_item_good(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        category_name = 'catname'
        item_name = 'item_name'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_read_item_val', return_value=async_mock('bla')) as readpatch:
            ret_val = await c_mgr.get_category_item(category_name, item_name)
            assert 'bla' == ret_val
        readpatch.assert_called_once_with(category_name, item_name)

    @pytest.mark.asyncio
    async def test_get_category_item_bad(self, reset_singleton):
        category_name = 'catname'
        item_name = 'item_name'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_item_val', side_effect=Exception()) as readpatch:
                with pytest.raises(Exception):
                    await c_mgr.get_category_item(category_name, item_name)
            readpatch.assert_called_once_with(category_name, item_name)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to get category item based on category_name %s and item_name %s', 'catname', 'item_name')

    @pytest.mark.asyncio
    async def test_get_category_item_value_entry_good(self, reset_singleton):

        async def async_mock(return_value):
            return return_value

        category_name = 'catname'
        item_name = 'item_name'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(ConfigurationManager, '_read_value_val', return_value=async_mock('bla')) as readpatch:
            ret_val = await c_mgr.get_category_item_value_entry(category_name, item_name)
            assert 'bla' == ret_val
        readpatch.assert_called_once_with(category_name, item_name)

    @pytest.mark.asyncio
    async def test_get_category_item_value_entry_bad(self, reset_singleton):
        category_name = 'catname'
        item_name = 'item_name'
        storage_client_mock = MagicMock(spec=StorageClientAsync)
        c_mgr = ConfigurationManager(storage_client_mock)
        with patch.object(_logger, 'exception') as log_exc:
            with patch.object(ConfigurationManager, '_read_value_val', side_effect=Exception()) as readpatch:
                with pytest.raises(Exception):
                    await c_mgr.get_category_item_value_entry(category_name, item_name)
            readpatch.assert_called_once_with(category_name, item_name)
        assert 1 == log_exc.call_count
        log_exc.assert_called_once_with('Unable to get the "value" entry based on category_name %s and item_name %s', 'catname', 'item_name')

    @pytest.mark.asyncio
    async def test__create_new_category_good(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'response': [{'category_name': 'catname', 'category_val': 'catval', 'description': 'catdesc'}]}

        async def async_mock(return_value):
            return return_value

        category_name = 'catname'
        category_val = 'catval'
        category_description = 'catdesc'

        attrs = {"insert_into_tbl.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)

        with patch.object(AuditLogger, '__init__', return_value=None):
            with patch.object(AuditLogger, 'information', return_value=async_mock(None)) as auditinfopatch:
                with patch.object(PayloadBuilder, '__init__', return_value=None):
                    with patch.object(PayloadBuilder, 'INSERT', return_value=PayloadBuilder) as pbinsertpatch:
                        with patch.object(PayloadBuilder, 'payload', return_value=None) as pbpayloadpatch:
                            await c_mgr._create_new_category(category_name, category_val, category_description)
                        pbpayloadpatch.assert_called_once_with()
                    pbinsertpatch.assert_called_once_with(description=category_description, key=category_name, value=category_val)
            auditinfopatch.assert_called_once_with('CONAD', {'category': category_val, 'name': category_name})
        storage_client_mock.insert_into_tbl.assert_called_once_with(
            'configuration', None)

    @pytest.mark.asyncio
    async def test__read_all_category_names_1_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {
            'rows': [{'key': 'key1', 'description': 'description1'}]}

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)

        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_all_category_names()
        args, kwargs = storage_client_mock.query_tbl_with_payload.call_args
        assert 'configuration' == args[0]
        p = json.loads(args[1])
        assert {"return": ["key", "description", "value", {"column": "ts", "alias": "timestamp", "format": "YYYY-MM-DD HH24:MI:SS.MS"}]} == p
        assert [('key1', 'description1')] == ret_val

    @pytest.mark.asyncio
    async def test__read_all_category_names_2_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': [
            {'key': 'key1', 'description': 'description1'}, {'key': 'key2', 'description': 'description2'}]}

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_all_category_names()
        args, kwargs = storage_client_mock.query_tbl_with_payload.call_args
        assert 'configuration' == args[0]
        p = json.loads(args[1])
        assert {"return": ["key", "description", "value", {"column": "ts", "alias": "timestamp", "format": "YYYY-MM-DD HH24:MI:SS.MS"}]} == p
        assert [('key1', 'description1'), ('key2', 'description2')] == ret_val

    @pytest.mark.asyncio
    async def test__read_all_category_names_0_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': []}

        category_name = 'catname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_all_category_names()
        args, kwargs = storage_client_mock.query_tbl_with_payload.call_args
        assert 'configuration' == args[0]
        p = json.loads(args[1])
        assert {"return": ["key", "description", "value", {"column": "ts", "alias": "timestamp", "format": "YYYY-MM-DD HH24:MI:SS.MS"}]} == p
        assert [] == ret_val

    @pytest.mark.asyncio
    async def test__read_category_val_1_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': [{'value': 'value1'}]}
        category_name = 'catname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)

        with patch.object(PayloadBuilder, '__init__', return_value=None):
            with patch.object(PayloadBuilder, 'SELECT', return_value=PayloadBuilder) as pbselectpatch:
                with patch.object(PayloadBuilder, 'WHERE', return_value=PayloadBuilder) as pbwherepatch:
                    with patch.object(PayloadBuilder, 'payload', return_value=None) as pbpayloadpatch:
                        ret_val = await c_mgr._read_category_val(category_name)
                        assert 'value1' == ret_val
                    pbpayloadpatch.assert_called_once_with()
                pbwherepatch.assert_called_once_with(["key", "=", category_name])
            pbselectpatch.assert_called_once_with('value')
        storage_client_mock.query_tbl_with_payload.assert_called_once_with(
            'configuration', None)

    @pytest.mark.asyncio
    async def test__read_category_val_0_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': []}

        category_name = 'catname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)

        with patch.object(PayloadBuilder, '__init__', return_value=None):
            with patch.object(PayloadBuilder, 'SELECT', return_value=PayloadBuilder) as pbselectpatch:
                with patch.object(PayloadBuilder, 'WHERE', return_value=PayloadBuilder) as pbwherepatch:
                    with patch.object(PayloadBuilder, 'payload', return_value=None) as pbpayloadpatch:
                        ret_val = await c_mgr._read_category_val(category_name)
                        assert ret_val is None
                    pbpayloadpatch.assert_called_once_with()
                pbwherepatch.assert_called_once_with(["key", "=", category_name])
            pbselectpatch.assert_called_once_with('value')
        storage_client_mock.query_tbl_with_payload.assert_called_once_with(
            'configuration', None)

    @pytest.mark.asyncio
    async def test__read_item_val_0_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': []}

        category_name = 'catname'
        item_name = 'itemname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_item_val(category_name, item_name)
        assert ret_val is None

    @pytest.mark.asyncio
    async def test__read_item_val_1_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': [{'value': 'value1'}]}

        category_name = 'catname'
        item_name = 'itemname'
        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_item_val(category_name, item_name)
        assert ret_val == 'value1'

    @pytest.mark.asyncio
    async def test__read_value_val_0_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': []}

        category_name = 'catname'
        item_name = 'itemname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_value_val(category_name, item_name)
        assert ret_val is None

    @pytest.mark.asyncio
    async def test__read_value_val_1_row(self, reset_singleton):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {'rows': [{'value': 'value1'}]}

        category_name = 'catname'
        item_name = 'itemname'

        attrs = {"query_tbl_with_payload.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)
        ret_val = await c_mgr._read_value_val(category_name, item_name)
        assert ret_val == 'value1'

    @pytest.mark.asyncio
    async def test__update_value_val(self, reset_singleton):
        async def async_mock(return_value):
            return return_value

        category_name = 'catname'
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {"rows": []}

        item_name = 'itemname'
        new_value_val = 'newval'

        attrs = {"query_tbl_with_payload.return_value": mock_coro(), "update_tbl.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)

        with patch.object(AuditLogger, '__init__', return_value=None):
            with patch.object(AuditLogger, 'information', return_value=async_mock(None)) as auditinfopatch:
                await c_mgr._update_value_val(category_name, item_name, new_value_val)
        auditinfopatch.assert_called_once_with(
            'CONCH', {
                'category': category_name, 'item': item_name, 'oldValue': None, 'newValue': new_value_val})

    @pytest.mark.asyncio
    async def test__update_category(self, reset_singleton, mocker):
        @asyncio.coroutine
        def mock_coro(*args, **kwargs):
            return {"response": "dummy"}

        category_name = 'catname'
        category_description = 'catdesc'
        category_val = 'catval'

        attrs = {"update_tbl.return_value": mock_coro()}
        storage_client_mock = MagicMock(spec=StorageClientAsync, **attrs)
        c_mgr = ConfigurationManager(storage_client_mock)

        with patch.object(PayloadBuilder, '__init__', return_value=None):
            with patch.object(PayloadBuilder, 'SET', return_value=PayloadBuilder) as pbsetpatch:
                with patch.object(PayloadBuilder, 'WHERE', return_value=PayloadBuilder) as pbwherepatch:
                    with patch.object(PayloadBuilder, 'payload', return_value=None) as pbpayloadpatch:
                        await c_mgr._update_category(category_name, category_val, category_description)
                    pbpayloadpatch.assert_called_once_with()
                pbwherepatch.assert_called_once_with(["key", "=", category_name])
            pbsetpatch.assert_called_once_with(description='catdesc', value='catval')

        storage_client_mock.update_tbl.assert_called_once_with('configuration', None)
