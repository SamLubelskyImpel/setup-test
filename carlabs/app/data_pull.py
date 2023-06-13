from etl.sales_history import SalesHistoryETL
from etl.repair_order import RepairOrderETL
from datetime import datetime
from utils import load_progress
import logging
import os


_logger = logging.getLogger(__name__)
_logger.setLevel(os.environ['LOGLEVEL'])


def sales_history(event, context):
    if datetime.utcnow().hour == 4:
        return { 'etl_finished': True }

    last_id = load_progress('sales_history_progress')
    limit = 20

    etl = SalesHistoryETL(
        last_id=last_id,
        limit=limit)
    etl.run()

    _logger.info(f'ETL loaded={etl.loaded}, failed={etl.failed}')

    return {
        'etl_finished': etl.finished
    }


def repair_order(event, context):
    if datetime.utcnow().hour == 4:
        return { 'etl_finished': True }

    last_id = load_progress('repair_order_progress')
    limit = 1000

    etl = RepairOrderETL(
        last_id=last_id,
        limit=limit)
    etl.run()

    _logger.info(f'ETL loaded={etl.loaded}, failed={etl.failed}')

    return {
        'etl_finished': etl.finished
    }
