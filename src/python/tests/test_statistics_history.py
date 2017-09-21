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
                                        _list_stats_keys, _insert_into_stats_history,
                                        _update_previous_value, _select_from_statistics,
                                        stats_history_main)

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_CONNECTION_STRING = "dbname='foglamp'"
_KEYS = []

pytestmark = pytest.mark.asyncio

async def set_in_keys():
    """Set statistics.keys column into a list to be used by test cases"""
    stmt = sa.select([_STATS_TABLE.c.key])
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    _KEYS.append(result[0].replace(" ", ""))
    except Exception:
        raise

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
        os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(set_in_keys())

    def teardown_method(self):
        """Set up each test with fresh data, and empty _KEYS dictionary"""
        _KEYS.clear()
        os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")

    async def test_get_key_list(self):
        """
        Test that_get_key_list function works properly by comparing to the
        initial data
        :assert:
             The sorted list of keys returned is equal to the keys currently retreived
        """
        result = _list_stats_keys()
        assert sorted(result) == sorted(_KEYS)

    async def test_insert_into_stats_history(self):
        """
        Test that _insert_into_stats_history updates the value per key
        :assert:
            Assert statistics_history.value == rand_value for a given key
        """
        for key in _KEYS:
            stmt = sa.select([_STATS_HISTORY_TABLE.c.value]).where(
                _STATS_HISTORY_TABLE.c.key == key)
            rand_value = random.randint(1, 10)
            _insert_into_stats_history(key=key, value=rand_value,
                                       history_ts=str(datetime.datetime.now()))
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0] == rand_value
            except Exception:
                raise

    async def test_update_previous_value(self):
        """
        Test the update of statistics.previous_value
        :assert:
            Assert  statistics.previous_value  gets updated with the set value
        """
        for key in _KEYS:
            stmt = sa.select([_STATS_TABLE.c.value, _STATS_TABLE.c.previous_value]).where(
                _STATS_TABLE.c.key == key)
            rand_value = random.randint(1, 10)
            _update_previous_value(key=key, value=rand_value)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0] == 0
                            assert result[1] == rand_value
            except Exception:
                raise

    async def test_select_from_statistics(self):
        """
        Test that _select_from_statistics retrieves value and previous value from statistics table
        :assert:
            Assert that both value and previous value == 0
        """
        for key in _KEYS:
            result = _select_from_statistics(key=key)
            assert result[0][0] == 0
            assert result[0][0] == result[0][1]

    async def test_stats_history_main(self):
        """
        Test that as a whole (statistics_history_main) executes properaly
        :assert:
            values in statistics.value, statistics.previous_value, and
            statistics_history.value get updated
        """
        random_key = random.choice(_KEYS)
        rand_value = random.randint(1, 10)
        await update_statistics_value(statistics_key=random_key, value_increment=rand_value)
        stats_history_main()
        for key in _KEYS:
            stmt1 = sa.select([_STATS_TABLE.c.value, _STATS_TABLE.c.previous_value]).where(
                _STATS_TABLE.c.key == key)
            stmt2 = sa.select([_STATS_HISTORY_TABLE.c.value]).where(
                _STATS_HISTORY_TABLE.c.key == key)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt1):
                            if key is random_key:
                                assert result[0] == rand_value
                                assert result[1] == rand_value
                            else:
                                assert result[0] == 0
                                assert result[1] == 0
                        async for result in conn.execute(stmt2):
                            if key is random_key:
                                assert result[0] == rand_value
                            else:
                                assert result[0] == 0
            except Exception:
                raise
