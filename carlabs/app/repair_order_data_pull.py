import logging
import os
from repair_order_etl import RepairOrderETLProcess
from datetime import datetime, timedelta


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])

QUERY_LIMIT=100


def lambda_handler(event, context):
    iteration_number = event.get('iteration_number', 0)
    _logger.info(f'running iteration {iteration_number}')

    has_more_data = RepairOrderETLProcess(
        limit=QUERY_LIMIT,
        day=(datetime.today()-timedelta(days=1)).date()).run()
    iteration_number += 1

    return {
        'iteration_number': iteration_number,
        'keep_data_pull': has_more_data
    }

