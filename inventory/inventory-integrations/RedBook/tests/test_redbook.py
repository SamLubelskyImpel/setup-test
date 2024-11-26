import unittest
from unittest.mock import patch, Mock, MagicMock
import json
import os
import sys
parent_dir_name = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir_name + "/app")

from retrieve_options import lambda_handler, check_db, fetch_redbook_data, process, refresh_token, save_token, retrieve_token, save_data_to_db


class TestRetrieveOptions(unittest.TestCase):

    def test_check_db(self):
        mock_cursor = Mock(name="mock_cursor")
        mock_rbc = '111111'
        mock_cursor.fetchall.return_value = [("Option1",), ("Option2",)]
        
        result = check_db(mock_cursor, mock_rbc)
        self.assertEqual(result, ["Option1", "Option2"])
        mock_cursor.fetchall.assert_called_once()



    def test_fetch_redbook_data(self):
        with patch('retrieve_options.retrieve_token', return_value="mocked_token") as mock_retrieve_token:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"results": [{"equipmentname": "Equipment1"}]}
            with patch('requests.get', return_value = mock_response) as mock_get:
                result = fetch_redbook_data(4, "578555")
                
                self.assertEqual(result, {"results": [{"equipmentname": "Equipment1"}]})
                mock_retrieve_token.assert_called_once()
                mock_get.assert_called_once()

    @patch('retrieve_options.get_connection')
    @patch('retrieve_options.check_db')
    @patch('retrieve_options.fetch_redbook_data')
    @patch('retrieve_options.process')
    @patch('retrieve_options.save_data_to_db')
    @patch('retrieve_options.logger')
    def test_lambda_handler_db_success(self, mock_logger, mock_save_to_db, mock_process, mock_fetch_data, mock_check_db, mock_get_connection):
        # Mock event and context
        event = {"redbookCode": "578555"}
        context = {}

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock check_db to return some data
        mock_check_db.return_value = [{"equipmentname": "Test Equipment"}]
        
        # Mock fetch_redbook_data to simulate API call (not used if DB data exists)
        mock_fetch_data.return_value = {}

        # Expected return value when data is fetched from DB
        expected_result = json.dumps({"success": True, "results": [{"equipmentname": "Test Equipment"}]})

        # Call lambda_handler
        result = lambda_handler(event, context)

        # Assertions
        self.assertEqual(result, expected_result)
        mock_check_db.assert_called_once_with(mock_cursor, "578555")
        mock_fetch_data.assert_not_called()  # Shouldn't be called because check_db returned results
        mock_save_to_db.assert_not_called()  # Shouldn't be called because the data was fetched from the DB
        mock_logger.info.assert_called_with("Returning data from DB: {'success': True, 'results': [{'equipmentname': 'Test Equipment'}]}")


    @patch('retrieve_options.get_connection')
    @patch('retrieve_options.check_db')
    @patch('retrieve_options.fetch_redbook_data')
    @patch('retrieve_options.process')
    @patch('retrieve_options.save_data_to_db')
    @patch('retrieve_options.logger')
    def test_lambda_handler_redbook_api_success(self, mock_logger, mock_save_to_db, mock_process, mock_fetch_data, mock_check_db, mock_get_connection):
        # Mock event and context
        event = {"redbookCode": "578555"}
        context = {}

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock check_db to return None (simulate DB miss)
        mock_check_db.return_value = None
        
        # Mock fetch_redbook_data to return valid data from the API
        mock_fetch_data.return_value = {"totalCount": 1, "results": [{"equipmentname": "Test Equipment"}]}
        
        # Mock process to return processed data
        mock_process.return_value = [{"equipmentname": "Processed Equipment"}]
        
        # Mock save_data_to_db to do nothing
        mock_save_to_db.return_value = None

        # Expected return value when data is fetched from RedBook API and processed
        expected_result = json.dumps({"success": True, "results": [{"equipmentname": "Processed Equipment"}]})

        # Call lambda_handler
        result = lambda_handler(event, context)

        # Assertions
        self.assertEqual(result, expected_result)
        mock_check_db.assert_called_once_with(mock_cursor, "578555")
        mock_fetch_data.assert_called_once_with(rbc="578555")
        mock_process.assert_called_once_with({"totalCount": 1, "results": [{"equipmentname": "Test Equipment"}]})
        mock_save_to_db.assert_called_once_with("578555", [{"equipmentname": "Processed Equipment"}], mock_conn, mock_cursor)
        mock_logger.info.assert_called_with("Returning retrieved data: {'success': True, 'results': [{'equipmentname': 'Processed Equipment'}]}")


    @patch('retrieve_options.get_connection')
    @patch('retrieve_options.check_db')
    @patch('retrieve_options.fetch_redbook_data')
    @patch('retrieve_options.process')
    @patch('retrieve_options.save_data_to_db')
    @patch('retrieve_options.logger')
    def test_lambda_handler_redbook_api_failure(self, mock_logger, mock_save_to_db, mock_process, mock_fetch_data, mock_check_db, mock_get_connection):
        # Mock event and context
        event = {"redbookCode": "578555"}
        context = {}

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock check_db to return None (simulate DB miss)
        mock_check_db.return_value = None
        
        # Mock fetch_redbook_data to return invalid data (e.g., no totalCount)
        mock_fetch_data.return_value = {"totalCount": 0}
        
        # Expected result when no valid data is returned from RedBook API
        expected_result = json.dumps({"success": False, "message": "No valid data was retrieved from RedBook's API"})

        # Call lambda_handler
        result = lambda_handler(event, context)

        # Assertions
        self.assertEqual(result, expected_result)
        mock_check_db.assert_called_once_with(mock_cursor, "578555")
        mock_fetch_data.assert_called_once_with(rbc="578555")
        mock_process.assert_not_called()
        mock_save_to_db.assert_not_called()
        mock_logger.warning.assert_called_with("No valid data from RedBook API for code 578555")

    @patch('retrieve_options.get_connection')
    @patch('retrieve_options.check_db')
    @patch('retrieve_options.fetch_redbook_data')
    @patch('retrieve_options.process')
    @patch('retrieve_options.save_data_to_db')
    @patch('retrieve_options.logger')
    def test_lambda_handler_exception(self, mock_logger, mock_save_to_db, mock_process, mock_fetch_data, mock_check_db, mock_get_connection):
        # Mock event and context
        event = {"redbookCode": "578555"}
        context = {}

        # Mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_connection.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock check_db to return None (simulate DB miss)
        mock_check_db.return_value = None
        
        # Mock fetch_redbook_data to raise an exception
        mock_fetch_data.side_effect = Exception("API failure")

        # Expected result when an exception is raised during RedBook API fetch
        expected_result = json.dumps({"success": False, "message": "API failure"})

        # Call lambda_handler
        result = lambda_handler(event, context)

        # Assertions
        self.assertEqual(result, expected_result)
        mock_check_db.assert_called_once_with(mock_cursor, "578555")
        mock_fetch_data.assert_called_once_with(rbc="578555")
        mock_process.assert_not_called()
        mock_save_to_db.assert_not_called()
        mock_logger.exception.assert_called_with("Error processing data from RedBook API: API failure")


if __name__ == '__main__':
    unittest.main()
