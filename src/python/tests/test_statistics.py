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
from foglamp.statistics import update_statistics_value, _statistics_tbl

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_CONNECTION_STRING = "dbname='foglamp'"
_KEYS = []

pytestmark = pytest.mark.asyncio

async def reset_statistics():
    """reset statistics table to be set to 0"""
    stmt = _statistics_tbl.update().values(value=0, previous_value=0)
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
        async with engine.acquire() as conn:
            await conn.execute(stmt)

async def set_in_keys():
    """Set statistics.keys column into a list to be used by test cases"""
    stmt = sa.select([_statistics_tbl.c.key])
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
        async with engine.acquire() as conn:
            async for result in conn.execute(stmt):
                _KEYS.append(result[0].replace(" ", ""))

@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
class TestStatistics:
    """
    Test the different components of src/python/foglamp/statistics_history.py
    """

    def setup_method(self):
        """
        Set up each test with fresh data, and _KEYS dictionary with
        values from statistics.key column
        """
        _KEYS.clear()
        asyncio.get_event_loop().run_until_complete(reset_statistics())
        asyncio.get_event_loop().run_until_complete(set_in_keys())

    def teardown_method(self):
        """Set up each test with fresh data, and empty _KEYS dictionary"""
        _KEYS.clear()
        asyncio.get_event_loop().run_until_complete(reset_statistics())

    async def test_update_value(self):
        """
        Test that statistics table gets updated
        :assert:
            1. value gets update with new rand_value
            2. previous value does not change
        """
        key = random.choice(_KEYS)
        rand_value = random.randint(1, 10)
        await update_statistics_value(statistics_key=key, value_increment=rand_value)
        stmt = sa.select([_statistics_tbl.c.value]).where(_statistics_tbl.c.key == key)
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == rand_value

    async def test_update_consecutive_values(self):
        """
        Test that with multiple values updates, new update gets added, rather
        than replaced
        :assert:
            _statistics_tbl.c.value == sum(rand_value) 
        """
        key = random.choice(_KEYS)
        rand_value = [random.randint(1,10), random.randint(1,10)]
        await update_statistics_value(statistics_key=key, value_increment=rand_value[0])
        await update_statistics_value(statistics_key=key, value_increment=rand_value[1])

        stmt = sa.select([_statistics_tbl.c.value]).where(_statistics_tbl.c.key == key)
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == sum(rand_value)

    async def test_update_statistics_value_empty_key(self):
        """ 
        Test error appears when statistics_key is missing
        :assert: 
            proper error is returned
        """
        with pytest.raises(TypeError) as error_exec:
            await update_statistics_value(value_increment=random.randint(1,10))
        assert ("TypeError: update_statistics_value() missing 1 required " +
                "positional argument: 'statistics_key'") in str(error_exec)

    async def test_update_statistics_value_empty_value(self):
        """
        Test error appears when value_increment is missing
        :assert: 
            proper error is returned
        """
        key = random.choice(_KEYS)
        with pytest.raises(TypeError) as error_exec:
            await update_statistics_value(statistics_key=key)
        assert ("TypeError: update_statistics_value() missing 1 required " +
                "positional argument: 'value_increment'") in str(error_exec)
