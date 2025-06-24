import os
import subprocess
import json
import shutil # For changing directories safely

def find_microservices(base_directory="."):
    """
    Identifies microservice directories by looking for 'deploy.sh' files within them.
    """
    microservices = []
    search_file = "deploy.sh"

    print(f"Identifying microservices in '{os.path.abspath(base_directory)}'...")
    print("-" * 30)

    for root, dirs, files in os.walk(base_directory):
        if search_file in files:
            microservices.append(root)

    return sorted(microservices)

def run_tests_and_get_coverage(service_path):
    """
    Runs pytest under coverage.py, generates a JSON report, and extracts total coverage.

    Args:
        service_path (str): The path to the microservice directory.

    Returns:
        tuple: A tuple containing (coverage_percentage, json_report_path).
               Returns (0, None) if testing fails or coverage data cannot be extracted.
    """
    original_cwd = os.getcwd()
    os.chdir(service_path)
    print(f"\n--- Running tests for microservice: {os.path.basename(service_path)} ---")

    coverage_data_file = ".coverage"
    coverage_json_file = "coverage.json" # Name for the output JSON report

    try:
        # 1. Clean up previous coverage data and report
        if os.path.exists(coverage_data_file):
            os.remove(coverage_data_file)
        if os.path.exists(coverage_json_file):
            os.remove(coverage_json_file)

        # 2. Run pytest using 'coverage run -m pytest'
        #    This collects coverage data into .coverage file
        #    --source=. : Tells coverage.py to only measure code in the current directory
        #                 (important to avoid measuring test files or installed libraries)
        run_command = [
            "coverage",
            "run",
            "--source=.", # Measure code in the current directory
            "-m",
            "pytest",
            "-q"
        ]
        run_result = subprocess.run(run_command, capture_output=True, text=True, check=False)

        if run_result.returncode != 0:
            print(f"Pytest (run under coverage) failed for {os.path.basename(service_path)}.")
            print("STDOUT:\n", run_result.stdout)
            print("STDERR:\n", run_result.stderr)
            return 0, None # Return 0 coverage on failure

        # 3. Generate JSON report from the collected data
        #    This command reads .coverage and outputs coverage.json
        report_command = [
            "coverage",
            "json",
            "-o", coverage_json_file, # Output file for JSON
            "--pretty-print" # Make JSON human-readable
        ]
        report_result = subprocess.run(report_command, capture_output=True, text=True, check=False)

        if report_result.returncode != 0:
            print(f"Failed to generate coverage JSON report for {os.path.basename(service_path)}.")
            print("STDOUT:\n", report_result.stdout)
            print("STDERR:\n", report_result.stderr)
            return 0, None # Return 0 coverage if report generation fails

        # 4. Parse the JSON report
        if os.path.exists(coverage_json_file):
            with open(coverage_json_file, 'r') as f:
                coverage_data = json.load(f)

            total_coverage = coverage_data.get('totals', {}).get('percent_covered', 0.0)
            print(f"Coverage report generated at: {os.path.join(service_path, coverage_json_file)}")
            return total_coverage, os.path.join(service_path, coverage_json_file)
        else:
            print(f"Coverage JSON file '{coverage_json_file}' not found after reporting for {os.path.basename(service_path)}.")
            return 0, None

    except FileNotFoundError:
        print("Error: 'coverage' or 'pytest' command not found. Make sure coverage.py and pytest are installed and in your PATH.")
        return 0, None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON report for {os.path.basename(service_path)}. Report might be malformed.")
        return 0, None
    except Exception as e:
        print(f"An unexpected error occurred while testing {os.path.basename(service_path)}: {e}")
        return 0, None
    finally:
        os.chdir(original_cwd) # Always change back to the original directory


if __name__ == "__main__":
    base_search_dir = "." # You can change this to a specific path if needed
    service_directories = find_microservices(base_search_dir)

    if not service_directories:
        print("No microservices found to test.")
        exit()

    all_service_coverage = {}

    for service_dir in service_directories:
        coverage, json_report_path = run_tests_and_get_coverage(service_dir)
        service_name = os.path.basename(service_dir)
        all_service_coverage[service_name] = {
            "coverage_percent": coverage,
            "json_report_path": json_report_path
        }
        print(f"--> {service_name} Coverage: {coverage:.2f}%")

    print("\n--- Summary of Microservice Coverages ---")
    for service, data in all_service_coverage.items():
        print(f"{service}: {data['coverage_percent']:.2f}% (Report: {data['json_report_path'] if data['json_report_path'] else 'N/A'})")
    
    # Print the average coverage
    total_coverage = sum(data['coverage_percent'] for data in all_service_coverage.values())
    average_coverage = total_coverage / len(all_service_coverage)
    print(f"\nAverage Coverage: {average_coverage:.2f}%")

    # Print number of services with coverage greater than 80%
    services_with_coverage_greater_than_80 = [service for service, data in all_service_coverage.items() if data['coverage_percent'] > 80]
    print(f"\nNumber of services with coverage greater than 80%: {len(services_with_coverage_greater_than_80)} / {len(all_service_coverage)}")
    print(f"Services: {services_with_coverage_greater_than_80}")