import csv
import logging
from io import StringIO
from os import environ
from xml.etree import ElementTree

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


EXPECTED_TAGS = [
    "id",
    "dealerid",
    "stock_number",
    "make",
    "model",
    "year",
    "retailprice",
    "clutch",
    "location",
    "modifieddate",
    "vin",
    "airconditioners",
    "isselfcontained",
    "haslevelingjacks",
    "issalespending",
    "lengthininches",
    "istoyhauler",
    "displayorder",
    "vehtype",
    "bodysubtype",
    "url",
    "images",
    "classid",
    "condition",
    "price",
    "msrp",
    "accessories",
    "noteshtml",
    "dealername",
    "address",
    "city",
    "state",
    "zipcode",
    "telephone",
    "email",
    "iscustom",
    "isclearance",
    "isconsignment",
    "isfeatured",
    "enginehours",
    "horsepower",
    "color",
    "description",
    "miles",
    "imageoverlaytext",
    "torque",
    "transmission",
    "category_name",
    "fueltype",
    "family",
    "status",
    "propulsion",
    "enginetext",
    "brand",
    "displacement",
    "weight",
]


def format_string(raw_string):
    """Remove line breaks for single line csv files."""
    if not isinstance(raw_string, str):
        return ""
    return raw_string.replace("\n", "").replace("\r", "")


def validate_csv(csv_string):
    """Validate csv data contains proper data."""
    csv_buffer = StringIO(csv_string)
    reader = csv.reader(csv_buffer)
    expected_column_count = len(EXPECTED_TAGS)
    for row_number, row in enumerate(reader):
        if len(row) != expected_column_count:
            raise RuntimeError(
                f"Invalid row at line {row_number} expected {expected_column_count} columns got {len(row)} with data {row}."
            )


def parse_dealerspike_xml(xml_string):
    """Parse elements from dealerspike xml"""
    tree = ElementTree.fromstring(xml_string)
    all_item_data = []
    unexpected_tags = []
    for item in tree.findall("item"):
        item_data = {}
        for child in item:
            if child.tag in EXPECTED_TAGS:
                child_text = format_string(child.text)
                item_data[child.tag] = child_text
            else:
                unexpected_tags.append(child.tag)
        all_item_data.append(item_data)
    if len(unexpected_tags) > 0:
        logger.warning(
            f"WARNING: Dealerspike unexpected {unexpected_tags} tags not in expected tags {EXPECTED_TAGS}"
        )
    return all_item_data


def convert_dealerspike_csv(xml_string):
    """Convert dealerspike XML data to CSV."""
    all_item_data = parse_dealerspike_xml(xml_string)
    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=EXPECTED_TAGS)
    csv_writer.writeheader()
    for item_data in all_item_data:
        csv_writer.writerow(item_data)
    csv_content = csv_buffer.getvalue()
    validate_csv(csv_content)
    return csv_content
