import logging
import os
from etl import SalesHistoryETLProcess


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])


def lambda_handler(event, context):
    iteration_number = event.get('iteration_number', 0)
    _logger.info(f'running iteration {iteration_number}')

    has_more_data = SalesHistoryETLProcess(limit=100, offset=iteration_number*200).run()
    iteration_number += 1

    return {
        'iteration_number': iteration_number,
        'keep_data_pull': has_more_data and iteration_number < 3
    }

