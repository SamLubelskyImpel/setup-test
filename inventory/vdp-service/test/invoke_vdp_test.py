"""This module contains tests for invoke_vdp.py"""

import os
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

app_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../app'))
sys.path.append(app_dir)

os.environ["ENVIRONMENT"] = "test"
os.environ["DOWNLOAD_VDP_QUEUE_URL"] = "test_queue_url"

from invoke_vdp import get_new_vdp_files

os.environ["ENVIRONMENT"] = "test"


@patch("invoke_vdp.logger")
def test_get_new_vdp_files(mock_logger):
    """Test get_new_vdp_files function"""
    # Mock inputs
    mock_sftp = MagicMock()
    folder_name = "test_folder"
    active_dealers = ["dealer1", "dealer2"]
    last_modified_time = datetime.now(timezone.utc) - timedelta(hours=1)

    # Mock SFTP file attributes
    mock_sftp.listdir_attr.return_value = [
        MagicMock(filename="dealer1_vdp_file1.csv", st_mtime=(last_modified_time.timestamp() + 10)),
        MagicMock(filename="dealer2_vdp_file2.csv", st_mtime=(last_modified_time.timestamp() + 20)),
        MagicMock(filename="dealer3_vdp_file3.csv", st_mtime=(last_modified_time.timestamp() + 30)),  # Inactive dealer
        MagicMock(filename="dealer1_vdp_file4.txt", st_mtime=(last_modified_time.timestamp() + 40)),  # Invalid extension
        MagicMock(filename="dealer1_file5.csv", st_mtime=(last_modified_time.timestamp() + 50)),  # Missing "_vdp"
        MagicMock(filename="dealer1_vdp_file6.csv", st_mtime=(last_modified_time.timestamp() - 10)),  # File modified before last_modified_time
    ]

    # Call get_new_vdp_files
    result = get_new_vdp_files(mock_sftp, folder_name, active_dealers, last_modified_time)

    # Expected result
    expected_result = [
        {
            "provider_dealer_id": "dealer1",
            "vdp_file": "dealer1_vdp_file1.csv",
            "modification_time": int(last_modified_time.timestamp() + 10),
        },
        {
            "provider_dealer_id": "dealer2",
            "vdp_file": "dealer2_vdp_file2.csv",
            "modification_time": int(last_modified_time.timestamp() + 20),
        },
    ]

    # Assertions
    assert result == expected_result
    mock_logger.warning.assert_any_call("Invalid file extension for file: dealer1_vdp_file4.txt. Only '.csv' files are allowed.")
    mock_logger.warning.assert_any_call("Skipping file: dealer3_vdp_file3.csv. Dealer ID 'dealer3' is not active.")
