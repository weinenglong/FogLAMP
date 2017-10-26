# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

""" Statistics API """

# import logging
from foglamp import logger
from foglamp.storage.storage import Storage
from foglamp.storage.payload_builder import PayloadBuilder

__author__ = "Ashwin Gopalakrishnan"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"


_logger = logger.setup(__name__)


async def _update_statistics_value(statistics_key, value_increment):
    # TODO: storage layer does not support SET [column_name=column_name+value]
    payload = PayloadBuilder().WHERE(["key", "=", statistics_key]).payload()
    result = Storage().query_tbl_with_payload("statistics", payload)
    previous_value = result["rows"][0]["value"]
    payload = PayloadBuilder().SET(value=previous_value + value_increment).WHERE(["key", "=", statistics_key]).payload()
    Storage().update_tbl("statistics", payload)


async def update_statistics_value(statistics_key, value_increment):
    """Update the value column only of a statistics row based on key

    Keyword Arguments:
    category_name -- statistics key value (required)
    value_increment -- amount to increment the value by

    Return Values:
    None
    """
    try:
        return await _update_statistics_value(statistics_key, value_increment)
    except:
        _logger.exception(
            'Unable to update statistics value based on statistics_key %s and value_increment %s'
            , statistics_key, value_increment)
        raise


# async def main():
#     await update_statistics_value('READINGS', 10)
#
# if __name__ == '__main__':
#     import asyncio
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(main())
