import logging
import os
import sys
from datetime import datetime, timezone
from json import loads
from urllib.parse import urlparse
from uuid import uuid4

parent_dir_name = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(parent_dir_name + "/app")
os.environ["KAWASAKI_DATA_BUCKET"] = ""
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

from dealerspike import convert_dealerspike_csv
from download_kawasaki import get_kawasaki_file, validate_xml_data

# TODO Test from the prod S3 config rather than the sample config once deployed.
with open("../sample_kawasaki_config.json", "r") as f:
    kawasaki_config = loads(f.read())

for web_provider, dealer_configs in kawasaki_config.items():
    total_configs = len(dealer_configs)
    working_configs = 0
    for dealer_config in dealer_configs:
        try:
            response_content = get_kawasaki_file(web_provider, dealer_config)
            validate_xml_data(response_content)
            csv_content = convert_dealerspike_csv(response_content)
            now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
            filename = f"{urlparse(dealer_config['web_url']).netloc}_{now.strftime('%Y%m%d')}_{str(uuid4())}.csv"
            if not os.path.exists("output"):
                os.makedirs("output")
            with open(f"output/{filename}", "w+") as f:
                f.write(csv_content)
            logger.info(f"Wrote output/{filename}")
            working_configs += 1
        except Exception as exc:
            logger.exception(
                f"Error running provider {web_provider} with config {dealer_config}"
            )
    logger.info(f"{working_configs} of {total_configs} configs work")
