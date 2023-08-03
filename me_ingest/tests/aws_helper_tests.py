import sys
sys.path.append('..')

from aws_helper import AwsHelper
from datatypes import Event
import unittest
import warnings

class TestAwsHelper(unittest.TestCase):

    def test_upper(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        result = True
        event = Event(remote_path='V236/230717/111649/AV11_V236_230717_111649_62100/nv0/AV11_V236_230717_111649_62100/audio_tags/10057_12523_medium.wav')
        event.audio_tag = "10057_12523_medium.wav"
        helper = AwsHelper(bucket_name = "mobileye-msbz-inputs",
                           local_download_folder = '/audio_tags/')
        if not helper.connect():
            result = False
        helper._get_event_timestamp_from_frame_id(event)
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
