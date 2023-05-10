import logging

import boto3
import watchtower


def get_logger():
    """Get cloudwatch logger"""
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO)
    cw_handler = watchtower.CloudWatchLogHandler(
        log_group="dms_upload_api",
        boto3_client=boto3.client("logs", region_name="us-east-1"),
    )
    _logger.addHandler(cw_handler)
    return _logger
