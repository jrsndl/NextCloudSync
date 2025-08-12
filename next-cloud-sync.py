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
    Loads the folder states from a specified source directory.

    This function attempts to read a JSON file from the given source directory.
    If the file is found and successfully read, its contents are parsed into a
    dictionary representing the folder states. If the file is not found or an
    error occurs during reading or parsing, appropriate log warnings are generated.
    Returns an empty dictionary if the file is absent or fails to load.

    Parameters:
        source_path (str): The path to the source directory where the folder states
            file is located.

    Returns:
        dict: A dictionary representation of the folder states. Returns an empty
            dictionary if the folder states file is not found or could not be
            processed.

    Raises:
        Exception: Any exception arising during reading or parsing is caught, and
            a log warning is generated instead of re-raising the exception.
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
    Saves the folder states to a JSON file in the provided source directory.

    This function serializes the provided folder states and writes them to a
    JSON file named with the value of `STATE_FILE_NAME` in the specified
    `source_path`. If the operation fails, it logs an error message with the
    details of the failure.

    Parameters:
    source_path: str
        The directory path where the folder state file will be saved.
    folder_states: dict
        A dictionary representing the states of folders to be saved.

    Raises:
    Exception
        If an error occurs during the file writing process, it will be logged
        but not re-raised.
    """
    state_file_path = os.path.join(source_path, STATE_FILE_NAME)
    try:
        with open(state_file_path, "w") as state_file_path:
            json.dump(folder_states, state_file_path, sort_keys=True, indent=4)
    except Exception as e:
        # Log error if state saving fails
        logging.error(f"Failed to save folder states for {state_file_path}: {e}", exc_info=True)

def configure_logging(log_level=logging.INFO, log_directory="."):
    """
    Configures the logging system for the application. This function sets up logging
    to output both to a file and the console. The log files are stored in a specified
    directory, and their names contain a timestamp for distinction.

    Args:
        log_level: The logging level to be set. Defaults to logging.INFO.
        log_directory: The directory where log files will be stored. Defaults to ".".

    Returns:
        None
    """
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(log_directory, f"folder_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=log_level,  # Set the base logging level
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
    Monitors a specified directory for valid source packages and synchronizes these packages to appropriate
    destinations by copying them once specific conditions such as stability checks and retry limits are satisfied.

    Summary:
    This function is designed to continuously monitor a source directory for valid package folders and synchronize
    them to predefined destination directories. It periodically checks the source directory, evaluates the stability
    and synchronization status of the packages, and attempts to copy them to their designated destinations.
    Detailed metadata and checksums ensure data integrity during synchronization. The function continues running
    with a specified time interval between scans and saves state information to allow for seamless recovery
    during successive iterations.

    Args:
        source_path (str): The source directory path to monitor for package folders.
        destination_bases (list[str]): A list of base paths representing destinations where the packages must be copied.
        check_interval (int, optional): Time interval in seconds for rechecking the source directory. Default is 10.
        stable_checks (int, optional): Number of consecutive checks required to confirm a package is stable. Default is 3.
        retry_copy (int, optional): Maximum number of retry attempts allowed for copying a package. Default is 2.
        ingest_prefix (str, optional): Prefix string determining the structure and sources to ingest packages. Default is "in/vendors".
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
                            # Those packages are not changing size and is not synced yet
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
    Finds all destination projects in the provided base directories.

    This function scans all provided base directories to identify and collect the
    names and paths of top-level folders within them. The function filters only
    base directories that exist in the filesystem before processing.

    Parameters:
    destination_bases: list[str]
        A list of directory paths to search for top-level projects.

    Returns:
    dict
        A dictionary where the keys are folder names representing projects and the
        values are their corresponding absolute paths.

    Raises:
    None
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
    Finds all source packages in a specified directory and organizes metadata for further processing.

    This function scans through directories following a specific naming convention, examines the
    contents of these directories, and checks against provided parameters to identify new or
    existing packages. The results are returned in a dictionary containing package metadata.

    Parameters:
        source_path: str
            Path to the source directory containing project-user folders.
        ingest_prefix: str
            Prefix to be added to the destination path segments.
        stable_checks: int
            Number of stable state verifications required before processing a package.
        retry_copy: int
            Maximum allowed retries for package copying attempts.
        destination_projects: dict
            Dictionary mapping project names to their corresponding destination paths.
        folder_states: dict
            Dictionary containing cached states of folders for checksum and synchronization validation.

    Returns:
        dict
            A dictionary where the keys are paths to source packages and the values are dictionaries
            containing package-specific metadata such as project name, user name, destination paths,
            checksum values, synchronized status, detected date-time, and retry counts.
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
        match = re.match(r'^([^-]*)-([^-]*)-In$', one_folder)
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
                    logging.info(f"New package detected: {one_folder} (Project: {project_name}, User: {user_name})")
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
    Find the actual destination path for a given project name from a list of base paths.

    This function traverses through the provided base paths to locate a directory
    containing the specified project name. It checks the top-level folder names within
    each base path to determine if the project exists. If a match is found, the full
    path to the project folder is returned; otherwise, it returns None.

    Args:
        destination_bases (list[str]): A list of base directory paths to search in.
        project_name (str): The name of the project to locate.

    Returns:
        str or None: The full path to the project directory if found; otherwise, None.

    Raises:
        None
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
    Computes a unique hash representing the state of all files within a specified folder.

    This function recursively traverses a folder and builds a set of all file paths relative
    to the folder root along with their corresponding file sizes. The state of the folder is
    then condensed into a unique hash representing its current state.

    Args:
        folder_path (str): The path to the folder whose state needs to be computed.

    Returns:
        int: A unique hash representing the state of the folder.

    Raises:
        None
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
    Copies a folder and its contents to a destination folder, skipping files that
    already exist in the destination with the same relative path and size.

    This function traverses the source folder, reproduces its structure in the
    destination, and efficiently copies files that are absent or modified relative
    to the destination state. Metadata such as timestamps are preserved during the
    copy process.

    Parameters:
    source: str
        The path to the source folder that needs to be copied.
    destination: str
        The path to the destination folder where the contents should be copied.
    destination_state: set[tuple[str, int]]
        A set containing tuples of the relative paths and sizes of files already
        present in the destination folder.

    Returns:
    bool
        Returns True if at least one file was copied, otherwise returns False.
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
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Monitor a directory for new folders and copy files.")
    parser.add_argument("source_directory", type=str,
                        help="The source directory to monitor.")
    parser.add_argument("destination_directories", type=str, nargs='+',
                        help="A list of base directories to search for matching destinations.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Set the logging level (DEBUG or INFO). Default is INFO.")
    parser.add_argument("--check_interval", type=int, default=3,
                        help="Time interval between checks (in seconds). Default is 3.")
    parser.add_argument("--number_of_checks", type=int, default=3,
                        help="Number of stability checks before copying. Default is 3.")
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.log_level.upper() == "DEBUG" else logging.INFO
    configure_logging(log_level=log_level, log_directory=f"{args.source_directory}/_synclogs")

    try:
        monitor_directory(
            source_path=args.source_directory,
            destination_bases=args.destination_directories,
            check_interval=args.check_interval
        )
    except Exception as e:
        logging.error(f"Failed to start monitoring: {e}")

