import logging
import os
import sys
from datetime import datetime, timezone
from json import loads

parent_dir_name = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir_name + "/app")
os.environ["KAWASAKI_DATA_BUCKET"] = ""
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

from download_kawasaki import get_kawasaki_file, validate_xml_data

from utils.xml_to_csv import convert_xml_to_csv

checked_id = None
if len(sys.argv) == 2:
    checked_id = int(sys.argv[1])

with open("../kawasaki_config.json", "r") as f:
    kawasaki_config = loads(f.read())

working_configs = 0
total_configs = 0
for web_provider, dealer_configs in kawasaki_config.items():
    for dealer_config in dealer_configs:
        if checked_id and int(dealer_config['impel_id']) != int(checked_id):
            continue
        try:
            total_configs += 1
            response_content = get_kawasaki_file(web_provider, dealer_config)
            validate_xml_data(response_content)
            csv_data = convert_xml_to_csv(response_content, web_provider)
            now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
            filename = f"{web_provider}_{dealer_config['impel_id']}.csv"
            if not os.path.exists("output"):
                os.makedirs("output")
            with open(f"output/{filename}", "w+") as f:
                f.write(csv_data)
            logger.info(f"Wrote output/{filename}")
            working_configs += 1
        except Exception as exc:
            logger.exception(
                f"Error running provider {web_provider} with config {dealer_config}"
            )
logger.info(f"{working_configs} of {total_configs} configs work")
