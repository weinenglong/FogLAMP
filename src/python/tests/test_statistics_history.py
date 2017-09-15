"""The following is testing for statistics_history"""
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

import datetime
import asyncio
import aiopg.sa
import pytest
import sqlalchemy as sa
from foglamp.statistics_history import (_STATS_TABLE, _STATS_HISTORY_TABLE,
                                        _list_stats_keys, _insert_into_stats_history,
                                        stats_history_main,
                                        __query_execution, _update_previous_value,
                                        _select_from_statistics)

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_CONNECTION_STRING = "dbname='foglamp'"


async def _delete_from_statistics():
    """DELETE data from table"""
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                await conn.execute('DELETE FROM foglamp.statistics')
                await conn.execute('DELETE FROM foglamp.statistics_history')
    except Exception:
        print('DELETE failed')
        raise


async def _insert_init_data()->dict:
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
            'UNSNPURGED': 'The number of readings that were purged from the buffer ' +
                          'before being sent',
            'DISCARDED': 'The number of readings discarded at the input side by FogLAMP, ' +
                         'i.e. discarded before being placed in the buffer. ' +
                         'This may be due to some error in the readings themselves.'}
    values = {}
    for key in info:
        stmt = _STATS_TABLE.insert().values(key=key, description=info[key],
                                            value=0, previous_value=0)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    await conn.execute(stmt)
        except Exception:
            print('INSERT failed')
            raise
        values[key] = 0
    return values


async def _update_statistics_value(values: dict)->dict:
    """
    Update values in statistics table
    :param values:
        values: "hard-coded" in dictionary correspond to key/value in foglamp.statistics
    :return:
        The updated values dictionary
    """
    for key in values.keys():
        stmt = sa.update(_STATS_TABLE).values(value=values[key]+1).where(
            _STATS_TABLE.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    await conn.execute(stmt)
        except Exception:
            print('Updated failed: %s' % stmt)
            raise
        values[key] = + 1
    return values


@pytest.mark.asyncio
@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
async def test_init_data():
    """
    Test that the functions defined for testing behave as expected
    :assert:
        1. existing data was removed statistics table
        2. init data were inserted -- both value and previous_value = 0
        3. update values gets executed -- value = 1 and previous_value = 0
    """
    await _delete_from_statistics()
    stmt = sa.select([sa.func.count()]).select_from(_STATS_TABLE)
    try:
        async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
            async with engine.acquire() as conn:
                async for result in conn.execute(stmt):
                    assert result[0] == 0
    except Exception:
        print('Query failed: %s' % stmt)
        raise

    values = await _insert_init_data()
    for key in values.keys():
        stmt = sa.select([_STATS_TABLE.c.value]).select_from(_STATS_TABLE).where(
            _STATS_TABLE.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == values[key]
        except Exception:
            print('Query failed: %s' % stmt)
            raise
        stmt = sa.select([_STATS_TABLE.c.previous_value]).select_from(_STATS_TABLE).where(
            _STATS_TABLE.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise

    values = await _update_statistics_value(values)
    for key in values.keys():
        stmt = sa.select([_STATS_TABLE.c.value]).select_from(_STATS_TABLE).where(
            _STATS_TABLE.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == values[key]
        except Exception:
            print('Query failed: %s' % stmt)
            raise
        stmt = sa.select([_STATS_TABLE.c.previous_value]).select_from(_STATS_TABLE).where(
            _STATS_TABLE.c.key == key)
        try:
            async with aiopg.sa.create_engine(_CONNECTION_STRING) as engine:
                async with engine.acquire() as conn:
                    async for result in conn.execute(stmt):
                        assert result[0] == 0
        except Exception:
            print('Query failed: %s' % stmt)
            raise

    await _delete_from_statistics()


@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
def test_get_key_list():
    """
    Test that_get_key_list function works properly
    :assert:
         The sorted list of keys returned is equal to the keys declared by _insert_init_data
    """
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_delete_from_statistics())
    expect = event_loop.run_until_complete(_insert_init_data())

    result = _list_stats_keys()

    assert sorted(list(expect.keys())) == sorted(result)

    event_loop.run_until_complete(_delete_from_statistics())


@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
def test_select_from_statistics():
    """
    Test that _select_from_statistics method works
    :assert:
        since _insert_init_data() sets both value and previous_value to 0,
        the test assert that the two equal
    """
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_delete_from_statistics())
    values = event_loop.run_until_complete(_insert_init_data())
    for key in values.keys():
        result = _select_from_statistics(key=key)
        assert result[0][0] == result[0][1]
    event_loop.run_until_complete(_delete_from_statistics())


@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
def test_insert_into_stats_history():
    """
    Test _insert_into_stats_history method
    :assert:
        1. 7 rows were inserted
        2. foglamp.statistics_history.value = 0
        3. foglamp.statistics_history.history_ts = declared valued
    due to assertion 3, if system and psql timestamps aren't sync test will fail
    """
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_delete_from_statistics())

    values = {'READINGS': None, 'BUFFERED': None, 'SENT': None, 'UNSENT': None,
              'PURGED': None, 'UNSNPURGED': None, 'DISCARDED': None}

    for key in values:
        values[key] = datetime.datetime.now()
        _insert_into_stats_history(key=key, value=0, history_ts=values[key])

    stmt = sa.select([sa.func.count()]).select_from(_STATS_HISTORY_TABLE)
    result = __query_execution(stmt=stmt)
    assert result.fetchall()[0][0] == 7

    for key in values:
        stmt1 = sa.select([_STATS_HISTORY_TABLE.c.value]).select_from(
            _STATS_HISTORY_TABLE).where(_STATS_HISTORY_TABLE.c.key == key)
        result = __query_execution(stmt=stmt1)
        assert result.fetchall()[0][0] == 0
        stmt2 = sa.select([_STATS_HISTORY_TABLE.c.history_ts]).select_from(
            _STATS_HISTORY_TABLE).where(_STATS_HISTORY_TABLE.c.key == key)
        result = __query_execution(stmt=stmt2)
        assert str(result.fetchall()[0][0]).split("+")[0] == str(values[key])

    event_loop.run_until_complete(_delete_from_statistics())


@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
def test_update_previous_value():
    """
    Test that previous_value in foglamp.statistics gets updated
    :assert:
        Update of previous_value of foglam.statistics
    """
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_delete_from_statistics())
    values = event_loop.run_until_complete(_insert_init_data())
    values = event_loop.run_until_complete(_update_statistics_value(values))

    for key in values.keys():
        stmt = sa.select([_STATS_TABLE.c.previous_value]).select_from(_STATS_TABLE).where(
            _STATS_TABLE.c.key == key)
        result = __query_execution(stmt=stmt)
        assert result.fetchall()[0][0] == 0
        _update_previous_value(key=key, value=values[key])
        result = __query_execution(stmt=stmt)
        assert result.fetchall()[0][0] == 1
    event_loop.run_until_complete(_delete_from_statistics())


@pytest.allure.feature("unit")
@pytest.allure.story("statistics history")
def test_stats_history_main():
    """
    Test full update of statistics_history table by calling _stat_history_main
    instead of each private method
    :assert:
        1. When starting no rows exist in statistics_history
        2. Check that statistics_history has rows
        3. both value and previous_value for statistics were updated
        4. value for statistics_history was updated
    """
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(_delete_from_statistics())
    values = event_loop.run_until_complete(_insert_init_data())
    values = event_loop.run_until_complete(_update_statistics_value(values))

    stmt = sa.select([sa.func.count()]).select_from(_STATS_HISTORY_TABLE)
    result = __query_execution(stmt=stmt)
    assert result.fetchall()[0][0] == 0

    stats_history_main()

    result = __query_execution(stmt=stmt)
    assert result.fetchall()[0][0] == 7

    for key in values.keys():
        result = _select_from_statistics(key=key)
        assert result[0][0] == values[key]
        assert result[0][1] == values[key]

        stmt = sa.select([_STATS_HISTORY_TABLE.c.value]).select_from(
            _STATS_HISTORY_TABLE).where(_STATS_HISTORY_TABLE.c.key == key)
        result = __query_execution(stmt=stmt)
        assert result.fetchall()[0][0] == 1
    event_loop.run_until_complete(_delete_from_statistics())
