import csv
import io


def convert_ari_csv(xml_data):
    """Convert ari XML data to CSV."""
    # TODO: Convert the xml data rather than placeholder csv
    csv_data = io.StringIO()
    csv_rows = [
        ["Name", "Age", "City"],
        ["Alice", "25", "New York"],
        ["Bob", "30", "Los Angeles"],
        ["Charlie", "28", "Chicago"],
    ]
    csv_writer = csv.writer(csv_data, quoting=csv.QUOTE_MINIMAL)
    for row in csv_rows:
        csv_writer.writerow(row)
    csv_content = csv_data.getvalue()
    raise RuntimeError("ARI not implemented yet")
    return csv_content
