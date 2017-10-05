"""The following is testing for statistics_history"""
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END
import asyncio
import datetime
import os
import pytest
import random
import sqlalchemy as sa
import aiopg.sa

from foglamp.statistics import update_statistics_value
from foglamp.statistics_history import (_STATS_TABLE, _STATS_HISTORY_TABLE,
                                        list_stats_keys, insert_into_stats_history,
                                        update_previous_value, select_from_statistics,
                                        stats_history_main)

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_CONNECTION_STRING = "dbname='foglamp'"
_KEYS = []

pytestmark = pytest.mark.asyncio

async def truncate_statistics_history():
    """delete data from statistics_history"""
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
        async with engine.acquire() as conn:
            await conn.execute(_STATS_HISTORY_TABLE.delete())

async def reset_statistics():
    """reset statistics table to be set to 0"""
    stmt = _STATS_TABLE.update().values(value=0, previous_value=0)
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
        async with engine.acquire() as conn:
            await conn.execute(stmt)

async def set_in_keys():
    """Set statistics.keys column into a list to be usessd by test cases"""
    stmt = sa.select([_STATS_TABLE.c.key])
    async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
        async with engine.acquire() as conn:
            async for result in conn.execute(stmt):
                _KEYS.append(result[0].replace(" ", ""))

@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
class TestStatisticsHistory:
    """
    Test the different components of src/python/foglamp/statistics.py
    """
    def setup_method(self):
        """
        Set up each test with fresh data, and _KEYS dictionary with
        values from statistics.key column
        """
        _KEYS.clear()
        asyncio.get_event_loop().run_until_complete(set_in_keys())
        asyncio.get_event_loop().run_until_complete(truncate_statistics_history())
        asyncio.get_event_loop().run_until_complete(reset_statistics())

    def teardown_method(self):
        """Set up each test with fresh data, and empty _KEYS dictionary"""
        asyncio.get_event_loop().run_until_complete(truncate_statistics_history())
        asyncio.get_event_loop().run_until_complete(reset_statistics())
        _KEYS.clear()

    async def test_get_key_list(self):
        """
        Test that_get_key_list function works properly by comparing to the
        initial data
        :assert:
             The sorted list of keys returned is equal to the keys currently retrieved
        """
        result = list_stats_keys()
        assert sorted(result) == sorted(_KEYS)

    async def test_statistics_history_main(self):
        """
        Test that when running statistics_history_main data gets
        updated correctly
        :assert:
            1. Assert statistics.value was updated to be rand_value
            2. Assert statistics.previous_value was updated
                to be equal to statistics.value since it's the first
                iteration
            3. Assert statistics_history.value was updated to delta
                of statistics.value - statistics.previous_value.
                Since statistics.previous_value is set to 0,
                statistics_history.value == statistics.value.
            4. Assert that statistics_history.history_ts is consistent
        """
        history_ts_result = []
        # set history_ts
        stmt = sa.select([sa.func.now()])
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    history_ts = result[0]

        # set statistics.value
        for key in _KEYS:
            rand_value = random.randint(1, 10)
            await update_statistics_value(statistics_key=key,
                                          value_increment=rand_value)
            value = select_from_statistics(key)[0]
            assert value == rand_value

        stats_history_main()

        for key in _KEYS:
            value, previous_value = select_from_statistics(key)
            # assert previous_value was update
            assert value == previous_value
            stmt = sa.select([_STATS_HISTORY_TABLE.c.value,
                          _STATS_HISTORY_TABLE.c.history_ts]).where(
                _STATS_HISTORY_TABLE.c.key == key)

            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == value
                        history_ts_result.append(result[1])
        assert all(map(lambda x: x == history_ts_result[0], history_ts_result)) is True

    async def test_statistics_history_multiple_updates(self):
        """
        Test the proper update of statistics_history.value after
        multiple updates to statistics.value
        :assert:
            statistics_history.value = statistics.value - statistics.previous_value
        """
        key = random.choice(_KEYS)
        rand_value = random.randint(1, 10)
        # initial update of statistics and statistics_history
        await update_statistics_value(statistics_key=key,
                                      value_increment=rand_value)

        update_previous_value(key=key, value=rand_value)
        previous_value = select_from_statistics(key)[1]

        # multiple updates of statistics.value
        for i in range(3):
            await update_statistics_value(statistics_key=key,
                                          value_increment=random.randint(1, 10))
        stats_history_main()
        value = select_from_statistics(key)[0]
        stmt = sa.select([_STATS_HISTORY_TABLE.c.value]).where(
            _STATS_HISTORY_TABLE.c.key == key)
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == value - previous_value

