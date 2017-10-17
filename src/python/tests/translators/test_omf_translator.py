import os
import pytest 
from foglamp.translators import omf_translator

#@pytest.allure.feature("unit")
#@pytest.allure.story("configuration manager")
class TestOMFTranslator:
    """
    The following class tests the following functions from foglamp.translators 
      def plugin_retrieve_info(stream_id):
      def plugin_init():
      def plugin_send(raw_data, stream_id):
      def plugin_shutdown():
    """ 
    def setup_method(self):
        raw_output = os.popen("foglamp start")
        output = raw_output.read()
        raw_output.close()

    def teardown_method(self):
        raw_output = os.popen("foglamp stop")
        output = raw_output.read()
        raw_output.close()
    
    def test_plugin_init(self): 
        omf_translator.plugin_init()

    def test_plugin_retreive_info_error(self):
        """
        Test behavior when omf_translator.plugin_retrieve_info() doesn't contain anythng
        :assert:
            Error gets returned
        """
        with pytest.raises(TypeError) as error_exec:
            omf_translator.plugin_retrieve_info()
        assert "plugin_retrieve_info() missing 1 required positional argument: 'stream_id'" in str(error_exec) 
    
    def test_plugin_retrieve_info(self):
        """
        Since as of now the `stream_id` does not affect 
        the output, the test compares the plugin_retrive_info
        with `stream_id` of 0 and 1
        :assert:
            plugin_retrive_info returns consistent info
        """ 
        output0 = omf_translator.plugin_retrieve_info(0)
        output1 = omf_translator.plugin_retrieve_info(1)
        for key in output0:
            if key == 'config':
                for key2 in output0[key]:
                    if key2 == 'StaticData':
                        for key3 in output0[key][key2]:
                            assert output0[key][key2][key3] == output1[key][key2][key3]
                    else:
                        assert output0[key][key2] == output1[key][key2] 
            else:
                assert output0[key] == output1[key] 

    def test_plugin_init_error(self): 
        with pytest.raises(AttributeError) as error_exec: 
            omf_translator.plugin_init()
        assert "'str' object has no attribute 'debug'" in str(error_exec)

    def test_plugin_send_empty_error(self):
        """
        Test error gets returned when 1 or more variables is missing
        :assert:
            TypeError when both raw_data and stream_id empty
        """ 
        with pytest.raises(TypeError) as error_exec:
            omf_translator.plugin_send()
        assert "TypeError: plugin_send() missing 2 required positional arguments: 'raw_data' and 'stream_id'" in str(error_exec)
    
    def test_plugin_send_invalid_value(self):
        omf_translator.plugin_send(raw_data='', stream_id=1)
        
   
        
