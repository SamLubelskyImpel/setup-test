import os
import subprocess
import json
import shutil # For changing directories safely
import xml.etree.ElementTree as ET

def ensure_coverage_reports_dir():
    """
    Creates and returns the path to the .coverage-reports directory.
    """
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".coverage-reports")
    os.makedirs(reports_dir, exist_ok=True)
    return reports_dir

def get_coverage_report_path(service_path):
    """
    Generates a standardized path for coverage reports based on the service path.
    
    Args:
        service_path (str): The path to the microservice directory.
        
    Returns:
        str: The formatted path for coverage reports.
    """
    # Convert the service path to a coverage report filename
    # Remove leading ./ if present and replace / with -
    service_parts = service_path.strip('./').split('/')
    report_name = '-'.join(service_parts)
    reports_dir = ensure_coverage_reports_dir()
    return reports_dir, report_name

def find_microservices(base_directory="."):
    """
    Identifies microservice directories by looking for 'deploy.sh' files within them.
    Returns paths relative to the base directory.
    """
    microservices = []
    search_file = "deploy.sh"

    print(f"Identifying microservices in '{os.path.abspath(base_directory)}'...")
    print("-" * 30)

    base_path = os.path.abspath(base_directory)
    for root, dirs, files in os.walk(base_directory):
        if search_file in files:
            # Convert to relative path from base directory
            rel_path = os.path.relpath(root, base_path)
            microservices.append(rel_path)

    return sorted(microservices)

def parse_coverage_xml(xml_file):
    """
    Parse the coverage XML file and extract the total coverage percentage.
    
    Args:
        xml_file (str): Path to the coverage XML file.
        
    Returns:
        float: The total coverage percentage.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        # Get the coverage percentage from the root element's attributes
        total_coverage = float(root.attrib.get('line-rate', 0)) * 100
        return total_coverage
    except (ET.ParseError, KeyError, ValueError) as e:
        print(f"Error parsing coverage XML file: {e}")
        return 0.0

def run_tests_and_get_coverage(service_path):
    """
    Runs pytest under coverage.py, generates a XML report, and extracts total coverage.

    Args:
        service_path (str): The path to the microservice directory.

    Returns:
        tuple: A tuple containing (coverage_percentage, xml_report_path).
               Returns (0, None) if testing fails or coverage data cannot be extracted.
    """
    original_cwd = os.getcwd()
    try:
        print(f"\n--- Running tests for microservice: {os.path.basename(service_path)} ---")

        coverage_base_path, coverage_file_name = get_coverage_report_path(service_path)
        coverage_data_file = f".reports/coverage-{coverage_file_name}.xml"
        print(f"Coverage data file: {coverage_data_file}")
        print(f"Coverage base path: {coverage_base_path}")

        # 2. Run pytest using 'coverage run -m pytest'
        run_command = [
            "coverage",
            "run",
            "-m",
            "pytest",
            service_path  # Run tests in current directory
        ]
        run_result = subprocess.run(run_command, capture_output=True, text=True, check=False)

        if run_result.returncode != 0:
            print(f"Pytest (run under coverage) failed for {os.path.basename(service_path)}.")
            print("STDOUT:\n", run_result.stdout)
            print("STDERR:\n", run_result.stderr)
            return 0, None

        # 3. Generate XML report for the collected data
        os.makedirs(coverage_base_path, exist_ok=True)
        report_command = [
            "coverage",
            "xml",
            "-o",
            coverage_data_file
        ]
        report_result = subprocess.run(report_command, capture_output=True, text=True, check=False)

        if report_result.returncode != 0:
            print(f"Failed to generate coverage XML report for {os.path.basename(service_path)}.")
            print("STDOUT:\n", report_result.stdout)
            print("STDERR:\n", report_result.stderr)
            return 0, None

        # 4. Parse the XML file
        if os.path.exists(coverage_data_file):
            total_coverage = parse_coverage_xml(coverage_data_file)
            print(f"Coverage report generated at: {coverage_data_file}")
            return total_coverage, coverage_data_file
        else:
            print(f"Coverage data file '{coverage_data_file}' not found after reporting.")
            return 0, None

    except FileNotFoundError as e:
        print(f"Error: Command not found or file access error: {e}")
        return 0, None
    except Exception as e:
        print(f"An unexpected error occurred while testing {os.path.basename(service_path)}: {e}")
        return 0, None
    finally:
        os.chdir(original_cwd)  # Always change back to the original directory


if __name__ == "__main__":
    base_search_dir = "."  # You can change this to a specific path if needed
    service_directories = find_microservices(base_search_dir)

    if not service_directories:
        print("No microservices found to test.")
        exit(1)

    all_service_coverage = {}

    for service_dir in service_directories:
        coverage, xml_report_path = run_tests_and_get_coverage(service_dir)
        service_name = os.path.basename(service_dir)
        all_service_coverage[service_name] = {
            "coverage_percent": coverage,
            "report_path": xml_report_path
        }
        print(f"--> {service_name} Coverage: {coverage:.2f}%")

    # Calculate statistics
    if all_service_coverage:
        total_coverage = sum(data['coverage_percent'] for data in all_service_coverage.values())
        average_coverage = total_coverage / len(all_service_coverage)
        services_with_coverage_greater_than_80 = [
            service for service, data in all_service_coverage.items() 
            if data['coverage_percent'] > 80
        ]

        # Write report to file
        with open('coverage_report.txt', 'w') as f:
            f.write("Coverage Report\n")
            f.write("==============\n\n")
            
            f.write("Service Coverage Details\n")
            f.write("----------------------\n")
            for service, data in all_service_coverage.items():
                f.write(f"{service}: {data['coverage_percent']:.2f}% (Report: {data['report_path'] if data['report_path'] else 'N/A'})\n")
            
            f.write("\nSummary Statistics\n")
            f.write("-----------------\n")
            f.write(f"Average Coverage: {average_coverage:.2f}%\n")
            f.write(f"Services with >80% Coverage: {len(services_with_coverage_greater_than_80)} / {len(all_service_coverage)}\n")
            f.write(f"High Coverage Services: {', '.join(services_with_coverage_greater_than_80)}\n")

        print(f"\nSummary:")
        print(f"Number of services with coverage greater than 80%: {len(services_with_coverage_greater_than_80)} / {len(all_service_coverage)}")
        print(f"Services with >80% coverage: {', '.join(services_with_coverage_greater_than_80)}")
        print("Coverage report has been written to coverage_report.txt")
    else:
        print("No coverage data was collected for any service.")