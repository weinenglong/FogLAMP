# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Statistics API """

import asyncio
import os
import random
import aiopg.sa
import pytest
import sqlalchemy as sa
from foglamp.statistics import Statistics

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
class TestStatistics:
    # TODO: FOGL-510 Hardcoded core_management_port needs to be removed, should be coming form a test configuration file
#    _core_management_port = 39940

#    _store = Storage("localhost", _core_management_port)
#    _readings = Readings("localhost", _core_management_port)

#    _CONFIG_CATEGORY_NAME = 'PURGE_READ'

    @classmethod
    @pytest.fixture(autouse=True)
    def _reset_db(cls):
        """Cleanup method, called after every test"""
#        yield
#        # Update statistics
#        payload = PayloadBuilder().SET(value=0, previous_value=0)
#        cls._store.update_tbl("statistics", payload)
        pass 

    def test_init(self):
        """
        Test error gets returned when Storage is invalid
        :assert:
            Assert TypeError gets returned
        """
        with pytest.raises(TypeError) as error_exec:
            Statistics()
        assert "TypeError: __init__() missing 1 required positional argument: 'storage'" in str(error_exec) 
    
    def test_update(self):
        """
        Test that value gets update 
        :assert:
            Assert that value gets updated
        """
        value_increment = random.randint(1,10) 
        # Get value before update
        _store.update(key="READINGS", value_increment=value_increment)
        # Get value after update
        # assert that current_value = previous_value + value_increment 
