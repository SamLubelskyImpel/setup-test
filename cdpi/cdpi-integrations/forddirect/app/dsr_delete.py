import logging
from os import environ
from json import loads, dumps

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())



def lambda_handler(event, context):
    logger.info("dsr_delete starting")
    logger.info(event)

    try:
        
        # TODO process and send answer to [TBD]/dsr/impel/delete/response

        return {
            "statusCode": 200,
            "body": dumps(
                {
                    "message": "Success"
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    except Exception as e:

        logger.exception(f"Error invoking ford direct dsr delete response {e}")
        
        return {
            "statusCode": 500,
            "body": dumps(
                {
                    "message": "Internal Server Error. Please contact Impel support."
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }