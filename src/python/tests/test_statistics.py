# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Statistics API """

# import logging
import asyncio
import random
import aiopg.sa
import pytest
import sqlalchemy as sa
import os
from foglamp.statistics import update_statistics_value

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_STATISTICS_TBL = sa.Table(
    'statistics',
    sa.MetaData(),
    sa.Column('key', sa.types.CHAR(10)),
    sa.Column('description', sa.types.VARCHAR(255)),
    sa.Column('value', sa.types.BIGINT),
    sa.Column('previous_value', sa.types.BIGINT),
    sa.Column('ts', sa.types.TIMESTAMP)
)

_CONNECTION_STRING = "dbname='foglamp'"
_KEYS = []

pytestmark = pytest.mark.asyncio

async def set_in_keys():
    stmt = sa.select([_STATISTICS_TBL.c.key])
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    _KEYS.append(result[0].replace(" ", ""))
    except Exception:
        raise

@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
class TestStatistics:
    def setup_method(self):
        _KEYS.clear()
        os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(set_in_keys())

    def teardown_method(self):
        _KEYS.clear()
        os.system("psql < `locate foglamp_ddl.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")
        os.system("psql < `locate foglamp_init_data.sql | grep 'FogLAMP/src/sql'` > /dev/null 2>&1")

    async def test_update_value(self):
        """
        Test that statistics table gets updated
        :assert:
            1. value gets update with new rand_value
            2. previous value does not change
        """
        for key in _KEYS:
            rand_value = random.randint(1, 10)
            await update_statistics_value(statistics_key=key, value_increment=rand_value)
            stmt = sa.select([_STATISTICS_TBL.c.value]).select_from(_STATISTICS_TBL).where(
                _STATISTICS_TBL.c.key == key)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0] == rand_value
            except Exception:
                raise

            stmt = sa.select([_STATISTICS_TBL.c.previous_value]).select_from(_STATISTICS_TBL).where(
                _STATISTICS_TBL.c.key == key)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0] == 0
            except Exception:
                raise

    async def test_update_consecutive_value(self):
        """
        Multiple Insert into table
        :assert:
            Assert that the value in statistics table is equal to the sum of value_increments
        """
        for key in _KEYS:
            rand_value = [random.randint(1, 10), random.randint(1, 10)]
            await update_statistics_value(statistics_key=key, value_increment=rand_value[0])
            await update_statistics_value(statistics_key=key, value_increment=rand_value[1])

            stmt = sa.select([_STATISTICS_TBL.c.value, _STATISTICS_TBL.c.previous_value]).select_from(_STATISTICS_TBL).where(
                _STATISTICS_TBL.c.key == key)
            try:
                async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                    async with engine.acquire() as conn:
                        async for result in conn.execute(stmt):
                            assert result[0] == sum(rand_value)
                            assert result[1] == 0
            except Exception:
                raise

    async def test_invalid_statistics_key(self):
        """
        Test what happens when statistics_key is invalid
        :assert:
            1. no new row/values has been added
            2. value and previous value for exists columns did not change
        """
        rand_value = random.randint(1, 10)
        await update_statistics_value(statistics_key='stats_key', value_increment=rand_value)
        stmt = sa.select([_STATISTICS_TBL.c.value]).where(_STATISTICS_TBL.c.key == 'stats_key')
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            raise

