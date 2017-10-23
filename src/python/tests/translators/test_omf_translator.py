#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""The following code tests the omf_translator connector""" 

import aiopg.sa
import asyncio
import os
import pytest 
from psycopg2 import DataError
from foglamp.translators import omf_translator

__author__ = "Ori Shadmon"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_CONNECTION_STRING="dbname='foglamp'"

# Tests for plugin_retrieve_plugin_info
def test_plugin_retrieve_plugin_info():
    """
    Test that omf_translator.plugin_retrieve_info return valid values
    :assert:
        Assert omf_translator.plugin_retrieve_info is as expected
    """ 
    expect = {
      'type': 'translator', 
      'config': {
        'OMFRetrySleepTime': 1, 
        'producerToken': 'omf_translator_0001', 
        'OMFMaxRetry': 5, 
        'OMFHttpTimeout': 30, 
        'URL': 'http://WIN-4M7ODKB0RH2:8118/ingress/messages', 
        'StaticData': {
          'Location': 'Palo Alto', 
          'Company': 'Dianomic'
        }
      }, 
      'interface': '1.0', 
      'version': '1.0.0', 
      'name': 'OMF Translator'
    }
    actual = omf_translator.plugin_retrieve_info(1)
    
    for key in expect:
        if key == 'config':
            for key2 in expect[key]:
                if key2 == 'StaticData': 
                    for key3 in expect[key][key2]: 
                        assert expect[key][key2][key3] == actual[key][key2][key3]
                else: 
                    assert expect[key][key2] == actual[key][key2]
        else: 
            assert expect[key] == actual[key]

def test_plugin_retrieve_plugin_info_errors():
    """ 
    For plugin_retrieve_plugin_info check errors 
    :assert: 
        1. TypeError when no value set for stream_id
        2. psycopg2.DataError when stream_id > len 10
    """ 
    with pytest.raises(TypeError) as error_exec:
        acutal = omf_translator.plugin_retrieve_info()
    assert "TypeError: plugin_retrieve_info() missing 1 required positional argument: 'stream_id'" in str(error_exec)
    with pytest.raises(DataError) as error_exec:
        actual = omf_translator.plugin_retrieve_info("stream_id_01")
    assert "psycopg2.DataError: value too long for type character(10)" in str(error_exec)

# Tests for plugin_send
def test_plugin_send():
    """
    When doing plugin_send and setting some values to `raw_data` an error 
    gets returned "unexpected keyword argument". I'm not entirely sure 
    whether whether this an issue with how I call the code 
    """
    values = [{}, [], (), "aaa", 'aaa']
    output=''
    for value in values: 
        with pytest.raises(TypeError) as error_exec:
            output = omf_translator.plugin_send(raw_data=value, stream_id=1)
        print(value, error_exec)
        print(output)

def test_plugin_send_error():
    """
    Test error gets returned when raw_data and stream_id are missing
    :assert:
        Assert TypeError returned
    """
    with pytest.raises(TypeError) as error_exec: 
        actual = omf_translator.plugin_send()
    assert "TypeError: plugin_send() missing 2 required positional arguments: 'raw_data' and 'stream_id'" in str(error_exec)

