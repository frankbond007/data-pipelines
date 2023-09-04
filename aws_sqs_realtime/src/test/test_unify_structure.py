import unittest
from datetime import datetime, timezone
from ..unify_structure import convert_to_utc, unify_structure

class TestConvertToUTC(unittest.TestCase):
    def test_convert_to_utc_with_integer_timestamp(self):
        timestamp = 1628388000  # 2021-08-08 12:00:00 UTC
        result = convert_to_utc(timestamp)
        self.assertEqual(result, "2021-08-08T12:00:00+00:00")

    def test_convert_to_utc_with_isoformat_string(self):
        timestamp = "2021-08-08T12:00:00Z"
        result = convert_to_utc(timestamp)
        self.assertEqual(result, "2021-08-08T12:00:00+00:00")

    def test_convert_to_utc_with_invalid_format(self):
        timestamp = "2021-08-08 12:00:00"  # Invalid format
        with self.assertRaises(ValueError):
            convert_to_utc(timestamp)

class TestUnifyStructure(unittest.TestCase):
    def test_unify_structure_with_valid_input(self):
        input_json = {
            'type': 'issue',
            'id': 123,
            'state': 'open',
            'title': 'Test Issue',
            'user': 'john_doe',
            'body': 'This is a test issue',
            'labels': ['bug', 'high-priority'],
            'created_at': 1628388000,
            'repository': 'user/repo'
        }

        expected_output = {
            'type': 'issue',
            'id': 123,
            'status': 'open',
            'title': 'Test Issue',
            'user': 'john_doe',
            'body': 'This is a test issue',
            'tags': ['bug', 'high-priority'],
            'created_or_started_at': '2021-08-08T12:00:00+00:00',
            'repository': {
                'name': 'repo',
                'owner': 'user'
            }
        }

        result = unify_structure(input_json)
        self.assertEqual(result, expected_output)

    def test_unify_structure_with_missing_data(self):
        input_json = {
            'type': 'issue',
            'id': 123,
            'title': 'Test Issue',
            'user': 'john_doe',
            'repository': 'user/repo'
        }

        expected_output = input_json  # The input is returned as is in case of missing data

        result = unify_structure(input_json)
        self.assertEqual(result, expected_output)

if __name__ == "__main__":
    unittest.main()
