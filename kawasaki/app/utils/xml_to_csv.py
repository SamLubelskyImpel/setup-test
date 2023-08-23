import csv
import io
import logging
from os import environ
from xml.etree import ElementTree

from .provider_configs import get_provider_config

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def parse_tag_data(child, provider_config):
    """Parse child tag data from xml main tag."""
    if provider_config["provider"] == "dx1":
        if child.tag == "listing-photos":
            url_text = []
            for url_tag in child.findall(".//url"):
                url_text.append(format_string(url_tag.text))
            if len(url_text) <= 0:
                return ""
            return url_text
        if child.tag == "specifications":
            specifications_text = []
            for specification_tag in child.findall(".//specification"):
                specification_name = format_string(
                    specification_tag.find(".//specification-name").text
                )
                specification_value = format_string(
                    specification_tag.find(".//specification-value").text
                )
                specifications_text.append(
                    f"{specification_name}|{specification_value}"
                )
            if len(specifications_text) <= 0:
                return ""
            return specifications_text
    return format_string(child.text)


def format_string(raw_string):
    """Remove line breaks for single line CSV files."""
    if not isinstance(raw_string, str):
        return ""
    return raw_string.replace("\n", "").replace("\r", "").strip()


def parse_xml(xml_string, provider_config):
    """Parse items from XML."""
    tree = ElementTree.fromstring(xml_string)
    all_item_data = []
    unexpected_tags = set()
    for item in tree.findall(provider_config["tag"]):
        item_data = {}
        for child in item:
            if child.tag in provider_config["headers"]:
                tag_data = parse_tag_data(child, provider_config)
                item_data[child.tag] = tag_data
            else:
                unexpected_tags.add(child.tag)
        all_item_data.append(item_data)
    if len(unexpected_tags) > 0:
        logger.warning(
            f"Unexpected {list(unexpected_tags)} tags not in expected tags {provider_config['headers']}"
        )
    return all_item_data


def convert_xml_to_csv(xml_string, provider):
    """Convert XML data to CSV."""
    provider_config = get_provider_config(provider)
    all_item_data = parse_xml(xml_string, provider_config)
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=provider_config["headers"])
    csv_writer.writeheader()
    for item_data in all_item_data:
        csv_writer.writerow(item_data)
    csv_content = csv_buffer.getvalue()
    return csv_content
