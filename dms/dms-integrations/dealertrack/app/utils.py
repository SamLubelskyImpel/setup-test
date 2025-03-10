from xml.etree import ElementTree
from concurrent.futures import ThreadPoolExecutor, as_completed


NAMESPACES = {
    'soap': 'http://www.w3.org/2003/05/soap-envelope',
    'opentrack': 'opentrack.dealertrack.com/transitional'
}


def get_xml_tag_text(xml_root, tag_name, is_ro=False):
    tag = get_xml_tag(xml_root, tag_name, first=True, is_ro=is_ro)
    return tag.text if tag is not None else None


def get_xml_tag(xml_root, tag_name, first=False, is_ro=False):
    if not xml_root:
        return None

    tag_search = f'.//{{opentrack.dealertrack.com}}{tag_name}' if is_ro else f'.//opentrack:{tag_name}'

    tag = xml_root.findall(tag_search, NAMESPACES)

    if first:
        return tag[0] if len(tag) else None

    return tag


def xml_to_string(xml_root):
    return ElementTree.tostring(xml_root, encoding="unicode") if xml_root else ""


def execute_ordered_threads(tasks: list, max_workers: int):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(*task): i for i, task in enumerate(tasks)}
        futures_results = [None] * len(tasks)

        for future in as_completed(futures):
            task_index = futures[future]
            futures_results[task_index] = future.result()

        return futures_results
