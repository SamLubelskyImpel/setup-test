'''
TODO this looks like pretty much identical to dealerspike.py,
we could redesign it to avoid duplication
'''
import csv
import io
from xml.etree import ElementTree


def format_string(raw_string):
    """Remove line breaks for single line csv files."""
    if not isinstance(raw_string, str):
        return ""
    return raw_string.replace("\n", "").replace("\r", "")


def parse_xml(xml_string):
    """Parse elements from ARI xml"""
    tree = ElementTree.fromstring(xml_string)
    all_item_data = []
    headers = set()
    for item in tree.findall("item"):
        item_data = {}
        for child in item:
            item_data[child.tag] = format_string(child.text)
            headers.add(child.tag)
        all_item_data.append(item_data)
    return all_item_data, headers


def convert_ari_csv(xml_string):
    """Convert ari XML data to CSV."""
    all_item_data, headers = parse_xml(xml_string)
    csv_buffer = io.StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=headers)
    csv_writer.writeheader()
    for item_data in all_item_data:
        csv_writer.writerow(item_data)
    csv_content = csv_buffer.getvalue()
    return csv_content
