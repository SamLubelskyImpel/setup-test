import csv
import io
import logging
from xml.etree import ElementTree
from .csv_headers import get_headers
from os import environ


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def format_string(raw_string):
    """Remove line breaks for single line CSV files."""
    if not isinstance(raw_string, str):
        return ""
    return raw_string.replace("\n", "").replace("\r", "")


def parse_xml(xml_string, headers):
    """Parse items from XML."""
    tree = ElementTree.fromstring(xml_string)
    all_item_data = []
    unexpected_tags = []
    for item in tree.findall("item"):
        item_data = {}
        for child in item:
            if child.tag in headers:
                child_text = format_string(child.text)
                item_data[child.tag] = child_text
            else:
                unexpected_tags.append(child.tag)
        all_item_data.append(item_data)
    if len(unexpected_tags) > 0:
        logger.warning(
            f"Unexpected {unexpected_tags} tags not in expected tags {headers}"
        )
    return all_item_data


def convert_xml_to_csv(xml_string, provider):
    """Convert XML data to CSV."""
    headers = get_headers(provider)
    all_item_data = parse_xml(xml_string, headers)
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=headers)
    csv_writer.writeheader()
    for item_data in all_item_data:
        csv_writer.writerow(item_data)
    csv_content = csv_buffer.getvalue()
    return csv_content
