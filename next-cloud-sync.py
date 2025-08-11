import argparse
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime

# File name for storing folder states
STATE_FILE_NAME = "folder_states.json"


def load_folder_states(source_path):
    """
    Load previously saved folder states from the file in the source directory.

    :param source_path: A list of directories where state files are stored.
    :return: A dictionary representing the folder states for each source directory.
    """
    folder_states = {}
    state_file_path = os.path.join(source_path, STATE_FILE_NAME)
    if os.path.exists(state_file_path):
        try:
            # Open the JSON file
            with open(state_file_path, 'r') as cache_file:
                data = cache_file.read()
            # Parse JSON data into a dictionary
            folder_states = json.loads(data)
        except Exception as e:
            # Log warning if file fails to load and skip this directory
            logging.warning(f"Failed to load folder states for {source_path}: {e}")
    else:
        logging.warning(f"The folder states file not found: {source_path}")

    return folder_states


def save_folder_states(source_path, folder_states):
    """
    Save the current folder states to a file in the source directory.

    :param source_path: A list of directories where state files should be stored.
    :param folder_states: A dictionary representing the folder states to save.
    """
    state_file_path = os.path.join(source_path, STATE_FILE_NAME)
    try:
        with open(state_file_path, "w") as state_file_path:
            json.dump(folder_states, state_file_path, sort_keys=True, indent=4)
    except Exception as e:
        # Log error if state saving fails
        logging.error(f"Failed to save folder states for {state_file_path}: {e}", exc_info=True)

def configure_logging():
    """
    Configures logging to write to both a file and the console.
    """
    log_filename = f"folder_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.DEBUG,  # Set the base logging level
        format="%(asctime)s - [%(levelname)s] - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),  # Log to file
            logging.StreamHandler()  # Log to console
        ],
    )
    logging.info("Logging initialized. Writing to log file and console.")


def monitor_directory(source_path, destination_bases, check_interval=10,
                      stable_checks=3, retry_copy=2, ingest_prefix="in/vendors"):
    """
    Monitors a source directory for changes, processes folders when their content remains stable
    for a specified number of checks, and then efficiently copies them to matching destination
    directories based on a predefined structure.
    """

    # read cached source folders
    folder_states = load_folder_states(source_path)
    destination_projects = find_all_destination_projects(destination_bases)

    logging.info(f"Starting to monitor directory: {source_path}")

    # Get a dictionary where keys are the absolute paths of valid source packages and
    # values are dictionaries containing metadata such as the `project_name`, `user_name`,
    # destination information, synchronization states, and the folder checksum.
    all_source_packages = find_all_source_packages(source_path, ingest_prefix, stable_checks, retry_copy, destination_projects, folder_states)
    logging.debug(f"Found {len(all_source_packages.keys())} valid source packages to check.")

    while True:
        try:
            logging.debug(f"Scanning source directory: {source_path}")
            # Process each folder in the source directory if ready for copying
            for pkg_path, pkg in all_source_packages.items():
                if not pkg.get("is_synced_to_destination", False):
                    if pkg.get("stable_checks", 0) > stable_checks:
                        if retry_copy > pkg.get("copy_retry_count", 0):
                            # this packages is not changing size and is not synced yet
                            # let's copy

                            destination = pkg.get("destination_package_path", "")
                            os.makedirs(destination, exist_ok=True)
                            logging.info(f"Copying files from {pkg_path} to {destination}...")
                            copied = copy_folder(pkg_path, destination, {})
                            if copied:
                                destination_checksum = get_folder_state(destination)
                                logging.debug(
                                    f"Comparing checksums {pkg.get('checksum')} {destination_checksum}")
                                if destination_checksum == pkg.get("checksum"):
                                    pkg["is_synced_to_destination"] = True
                                    pkg["copied_date_time"] = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    logging.info(f"Files copied from {pkg_path} to {destination}...")
                                else:
                                    pkg["is_synced_to_destination"] = False
                                    pkg["copy_retry_count"] += 1
                                    logging.info(f"Failed copying {pkg['copy_retry_count']} times from {pkg_path} to {destination}")
                            else:
                                pkg["is_synced_to_destination"] = False
                                pkg["copy_retry_count"] += 1
                                logging.info(f"Failed copying {pkg['copy_retry_count']} times from {pkg_path} to {destination}")
                        else:
                            logging.debug(
                                f"Package {pkg_path} exceeded number of copy retry counts {pkg['copy_retry_count']}. Skipping copying...")
                    else:
                        logging.debug(f"Package {pkg_path} copy skipped. {pkg['stable_checks']} {pkg['is_synced_to_destination']}")
                else:
                    logging.debug(f"Package {pkg_path} is already synced. Skipping copying...")

        except Exception as e:
            logging.error(f"Error during monitoring: {e}", exc_info=True)

        # save
        save_folder_states(source_path, all_source_packages)

        # sleep
        logging.debug(f"Sleeping for {check_interval} seconds.")
        time.sleep(check_interval)

        # scan the folders again, use current all_source_packages as a starting point
        destination_projects = find_all_destination_projects(destination_bases)
        all_source_packages = find_all_source_packages(source_path,
                                                       ingest_prefix,
                                                       stable_checks,
                                                       retry_copy,
                                                       destination_projects,
                                                       all_source_packages)

def find_all_destination_projects(destination_bases):
    """
    Find all destination projects based on the provided destination base directories. The function
    iterates through each directory in `destination_bases`, checks if the directory exists, and if
    it does, retrieves the list of top-level folders within the directory. These folders are then
    collated into a dictionary where the key is the top level directory and value is base directory plus
    top-level folder.

    :param destination_bases: A list of strings representing the base directories to be checked for 
        destination projects.
    :return: A dictionary where keys are the project names (top level dirs) and values are the base directory plus top-level folder (full path).
    """
    
    all_projects = {}
    for base in destination_bases:
        if os.path.exists(base):
            top_level_folders = {
                folder: os.path.join(base, folder)
                for folder in next(os.walk(base))[1]
            }
            all_projects.update(top_level_folders)

    return all_projects

def find_all_source_packages(source_path, ingest_prefix, stable_checks, retry_copy, destination_projects, folder_states):
    """
    Finds all valid source packages and their corresponding destination data, while
    filtering out folders that do not match naming conventions or are not associated
    with valid destination projects. Handles cached states for folder status and
    calculates checksums for unsynchronized or uncached packages.

    :param destination_projects: dick of existing cached destination projects
    :param stable_checks: how many checks before copying
    :param source_path: Root directory path for source folders. It contains project-user
        folders to be processed.
    :type source_path: str
    :param destination_bases: List of base directories for destination projects. Each
        directory in this list represents a location where processed folders should
        be transferred.
    :type destination_bases: list[str]
    :param ingest_prefix: A folder prefix to append in the construction of the
        destination folder hierarchy.
    :type ingest_prefix: str
    :return: A dictionary where keys are the absolute paths of valid source packages and
        values are dictionaries containing metadata such as the `project_name`, `user_name`,
        destination information, synchronization states, and the folder checksum.
    :rtype: dict
    """

    ingest_prefix = ingest_prefix.replace("\\", "/")

    # find all project-user folders at source place
    # skip badly named folders and projects with non-existent destination
    all_packages = {}

    current_folders = {f for f in os.listdir(source_path) if
                       os.path.isdir(os.path.join(source_path, f))}
    logging.debug(f"Checking {len(current_folders)} project-user folders at {source_path}...")
    logging.debug(f"Stable checks {stable_checks},  retry copy {retry_copy}")

    for one_folder in current_folders:
        match = re.match(r'^([^-]*)-([^-]*)$', one_folder)
        if match:
            project_name, user_name = match.groups()
            if project_name not in destination_projects.keys():
                # skip project names that do not exist in destination(s)
                continue

            one_folder_full_path = os.path.join(source_path, one_folder)
            project_user_folders = {f for f in os.listdir(one_folder_full_path) if
                               os.path.isdir(os.path.join(one_folder_full_path, f))}

            for one_package in project_user_folders:
                package_path = os.path.join(source_path, one_folder, one_package).replace("\\", "/")
                destination_project_path = destination_projects[
                    project_name].replace("\\", "/")
                destination_package_path = os.path.join(
                    destination_projects[project_name], *ingest_prefix.split("/"),
                    user_name, one_package).replace("\\", "/")
                package_detected_date_time = datetime.now().strftime(
                    "%Y%m%d_%H%M%S")
                if folder_states.get(package_path) is not None:
                    cached_package = folder_states.get(package_path)
                    # load stable_checks, is_synced_to_destination, checksum from cache
                    all_packages[package_path] = {
                        "project_name": project_name,
                        "user_name": user_name,
                        "destination_project_path": destination_project_path,
                        "destination_package_path": destination_package_path,
                        "stable_checks": cached_package.get("stable_checks", 0),
                        "is_synced_to_destination": cached_package.get("is_synced_to_destination", False),
                        "checksum": cached_package.get("checksum", ""),
                        "detected_date_time": cached_package.get("detected_date_time", package_detected_date_time),
                        "copied_date_time": cached_package.get("copied_date_time", ""),
                        "copy_retry_count": cached_package.get("copy_retry_count", 0),
                    }
                else:
                    # new package!
                    logging.debug(f"New package detected: {one_folder} (Project: {project_name}, User: {user_name})")
                    all_packages[package_path] = {
                        "project_name": project_name,
                        "user_name": user_name,
                        "destination_project_path": destination_project_path,
                        "destination_package_path": destination_package_path,
                        "stable_checks": 0,
                        "is_synced_to_destination": False,
                        "checksum": None,
                        "detected_date_time": package_detected_date_time,
                        "copied_date_time": "",
                        "copy_retry_count": 0,
                    }
                # only make a checksum if not synced and not provided by cache
                if not all_packages[package_path]["is_synced_to_destination"]:
                    if all_packages[package_path]["stable_checks"] <= stable_checks:
                        if all_packages[package_path]["copy_retry_count"] <= retry_copy:

                            # make a source checksum
                            current_checksum = get_folder_state(package_path)
                            if all_packages[package_path]["checksum"] == current_checksum:
                                all_packages[package_path]["stable_checks"] += 1
                                logging.debug(f"Package {package_path} checksum matches. Stable checks: {all_packages[package_path]['stable_checks']}.")
                            else:
                                all_packages[package_path]["stable_checks"] = 0
                                all_packages[package_path]["checksum"] = current_checksum
                                logging.debug(f"Package {package_path} checksum not matching. Stable checks: {all_packages[package_path]['stable_checks']}.")
                        else:
                            logging.debug(f"Package {package_path} exceeded number of copy retry counts {all_packages[package_path]['copy_retry_count']}. Skipping checksum calculation...")
                    else:
                        logging.debug(f"Package {package_path} exceeded number of stable checks {all_packages[package_path]['stable_checks']}. Skipping checksum calculation...")
            else:
                # name doesn't match the naming convention, skipping
                pass

    return all_packages






def find_actual_destination(destination_bases, project_name):
    """
    Finds the appropriate destination path for a given project name from the
    list of destination base paths.

    :param destination_bases: List of base paths to search in.
    :param project_name: The project name to match.
    :return: The first matching destination path or None.
    """
    for base in destination_bases:
        if os.path.exists(base):
            top_level_folders = set(
                next(os.walk(base))[1])  # Get top-level folder names
            if project_name in top_level_folders:
                return os.path.join(base, project_name)
            else:
                logging.debug(
                    f"No matching project '{project_name}' in destination base: {base}")
        else:
            logging.warning(f"Destination base path does not exist: {base}")
    return None


def get_folder_state(folder_path):
    """
    Gets the state of a folder by creating a hash of file paths and sizes.

    :param folder_path: Path of the folder to analyze.
    :return: A set of tuples (relative file path, file size).
    """
    state = set()
    for dirpath, _, filenames in os.walk(folder_path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            relative_path = os.path.relpath(filepath, folder_path)
            size = os.path.getsize(filepath)
            state.add((relative_path, size))
    state_hash = hash(frozenset(state))

    return state_hash


def copy_folder(source, destination, destination_state):
    """
    Copies only new or updated files from the source to the destination.

    :param source: Source directory to copy files from.
    :param destination: Destination directory to copy files to.
    :param destination_state: Current state of the destination folder.
    :return: True if files were copied, False otherwise.
    """
    copied = False
    for dirpath, _, filenames in os.walk(source):
        relative_dir = os.path.relpath(dirpath, source)
        dest_subdir = os.path.join(destination, relative_dir)

        # Create directories in the destination as needed
        os.makedirs(dest_subdir, exist_ok=True)

        for filename in filenames:
            source_file = os.path.join(dirpath, filename)
            dest_file = os.path.join(dest_subdir, filename)

            # Get file size and relative path
            relative_path = os.path.relpath(source_file, source)
            size = os.path.getsize(source_file)

            # Check if the file needs to be copied
            if (relative_path, size) not in destination_state:
                shutil.copy2(source_file, dest_file)  # Efficient file copy with metadata
                logging.debug(f"Copied file: {source_file} -> {dest_file}")
                copied = True

    return copied


if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Monitor a directory for new folders and copy files.")
    parser.add_argument("source_directory", type=str,
                        help="The source directory to monitor.")
    parser.add_argument("destination_directories", type=str, nargs='+',
                        help="A list of base directories to search for matching destinations.")
    parser.add_argument("--check_interval", type=int, default=3,
                        help="Time interval between checks (in seconds). Default is 3.")
    parser.add_argument("--number_of_checks", type=int, default=3,
                        help="Number of stability checks before copying. Default is 3.")
    args = parser.parse_args()

    try:
        monitor_directory(
            source_path=args.source_directory,
            destination_bases=args.destination_directories,
            check_interval=args.check_interval
        )
    except Exception as e:
        logging.error(f"Failed to start monitoring: {e}")

