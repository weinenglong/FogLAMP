# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Statistics API """

# import logging
import random
import aiopg.sa
import pytest
import sqlalchemy as sa

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

@pytest.fixture(scope="module")
async def _delete_from_statistics():
    """DELETE data from table"""
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                await conn.execute('DELETE FROM foglamp.statistics')
    except Exception:
        print('DELETE failed')
        raise

@pytest.fixture(scope="module")
async def _insert_init_data()->list:
    """
    Insert initial data into statistics table
    :return:
        Dictionary with key being foglamp.statistics.key and value being foglamp.statistics.value
    """
    info = {'READINGS': 'The number of readings received by FogLAMP since startup',
            'BUFFERED': 'The number of readings currently in the FogLAMP buffer',
            'SENT': 'The number of readings sent to the historian',
            'UNSENT': 'The number of readings filtered out in the send process',
            'PURGED': 'The number of readings removed from the buffer by the purge process',
            'UNSNPURGED': 'The number of readings that were purged from the buffer' +
                          ' before being sent',
            'DISCARDED': 'The number of readings discarded at the input side by ' +
                         'FogLAMP, i.e. discarded before being placed in the buffer. ' +
                         'This may be due to some error in the readings' +
                         'themselves.'}
    values = []
    for key in info:
        stmt = _STATISTICS_TBL.insert().values(key=key, description=info[key],
                                               value=0, previous_value=0)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    await conn.execute(stmt)
        except Exception:
            print('INSERT failed')
            raise
        values.append(key)
    return values


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
async def test_init_data():
    """
    Test that the initial data is actually being inserted
    :assert:
        1. existing data was removed statistics table
        2. init data was inserted
            - keys are as expected
            - value and previous value = 0
    """
    await _delete_from_statistics()
    stmt = sa.select([sa.func.count()]).select_from(_STATISTICS_TBL)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == 0
    except Exception:
        print('Query failed: %s' % stmt)
        raise

    expect_keys = await _insert_init_data()
    actual_keys = []
    stmt = sa.select([_STATISTICS_TBL.c.key]).select_from(_STATISTICS_TBL)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    actual_keys.append(result[0].replace(" ", ""))
    except Exception:
        print('Query failed: %s' % stmt)
        raise
    assert sorted(actual_keys) == sorted(expect_keys)

    for key in expect_keys:
        stmt = sa.select([_STATISTICS_TBL.c.value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        stmt = sa.select([_STATISTICS_TBL.c.previous_value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise

    await _delete_from_statistics()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
async def test_update_value():
    """
    Test that statistics table gets updated
    :assert:
        1. value gets update with new rand_value
        2. previous value does not change
    """
    await _delete_from_statistics()
    keys = await _insert_init_data()
    for key in keys:
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
            print('Query failed: %s' % stmt)
            raise

        stmt = sa.select([_STATISTICS_TBL.c.previous_value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise
    await _delete_from_statistics()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
async def test_update_consecutive_value():
    """
    Multiple Insert into table
    :assert:
        Assert that the value in statistics table is equal to the sum of value_increments
    """
    await _delete_from_statistics()
    keys = await _insert_init_data()
    for key in keys:
        rand_value = [random.randint(1, 10), random.randint(1, 10)]
        await update_statistics_value(statistics_key=key, value_increment=rand_value[0])
        await update_statistics_value(statistics_key=key, value_increment=rand_value[1])

        stmt = sa.select([_STATISTICS_TBL.c.value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == sum(rand_value)
        except Exception:
            print('Query failed: %s' % stmt)
            raise
    await _delete_from_statistics()


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("statistics")
async def test_invalid_update_error():
    """
    Test what happens when statistics_key is invalid
    :assert:
        1. no new row/values have been added
        2. value and previous value for exists columns did not change
    """
    await _delete_from_statistics()
    keys = await _insert_init_data()
    rand_value = random.randint(1, 10)
    await update_statistics_value(statistics_key='SEND', value_increment=rand_value)
    stmt = sa.select([sa.func.count()]).select_from(_STATISTICS_TBL).where(
        _STATISTICS_TBL.c.key == 'SEND')
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == 0
    except Exception:
        print('Query failed: %s' % stmt)
        raise

    for key in keys:
        stmt = sa.select([_STATISTICS_TBL.c.value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise

        stmt = sa.select([_STATISTICS_TBL.c.previous_value]).select_from(_STATISTICS_TBL).where(
            _STATISTICS_TBL.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise
    await _delete_from_statistics()
