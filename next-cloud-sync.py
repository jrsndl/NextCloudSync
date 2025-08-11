import argparse
from datetime import datetime
import json
import logging
import os
import re
import shutil
import time

# File name for storing folder states
STATE_FILE_NAME = "folder_states.json"


def load_folder_states(source_directories):
    """
    Load previously saved folder states from the file in the source directory.

    :param source_directories: A list of directories where state files are stored.
    :return: A dictionary representing the folder states for each source directory.
    """
    folder_states = {}
    for directory in source_directories:
        state_file_path = os.path.join(directory, STATE_FILE_NAME)
        if os.path.exists(state_file_path):
            try:
                with open(state_file_path, "r") as state_file:
                    folder_states[directory] = {k: {"state": set(v["state"]), "stable_count": v["stable_count"]} for k, v in json.load(state_file).items()}
            except Exception as e:
                # Log warning if file fails to load and skip this directory
                logging.warning(f"Failed to load folder states for {directory}: {e}")
    return folder_states


def save_folder_states(source_directories, folder_states):
    """
    Save the current folder states to a file in the source directory.

    :param source_directories: A list of directories where state files should be stored.
    :param folder_states: A dictionary representing the folder states to save.
    """
    for directory, states in folder_states.items():
        state_file_path = os.path.join(directory, STATE_FILE_NAME)
        try:
            with open(state_file_path, "w") as state_file:
                json.dump({k: {"state": list(v["state"]), "stable_count": v["stable_count"]} for k, v in states.items()}, state_file, indent=4)
        except Exception as e:
            # Log error if state saving fails
            logging.error(f"Failed to save folder states for {directory}: {e}", exc_info=True)


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
                      stable_checks=3, ingest_prefix="in/vendors"):
    """
    Monitors a source directory for changes, processes folders when their content remains stable
    for a specified number of checks, and then efficiently copies them to matching destination
    directories based on a predefined structure.
    """
    logging.info(f"Starting to monitor directory: {source_path}")
    logging.info(
        f"Destination base directories: {', '.join(destination_bases)}")

    # Persistent folder state storage
    folder_states = load_folder_states(source_path)
    logging.info(
        f"Loaded folder states: {len(folder_states)} folders from previous session.")

    destination_cache = {}  # Caches the state of destination folders
    seen_folders = set()




    while True:
        try:
            logging.debug(f"Scanning source directory: {source_path}")
            current_folders = {f for f in os.listdir(source_path) if
                               os.path.isdir(os.path.join(source_path, f))}

            # Process each folder in the source directory
            for folder in current_folders:
                folder_path = os.path.join(source_path, folder)

                # Skip folders already processed
                if folder in seen_folders:
                    continue

                logging.debug(f"Analyzing folder: {folder}")
                # Get current folder state (hashes based on file names and sizes)
                current_state = get_folder_state(folder_path)

                # Ensure source_path exists in folder_states
                if source_path not in folder_states:
                    folder_states[source_path] = {}

                if folder not in folder_states[source_path]:
                    logging.info(
                        f"New folder detected: {folder} in {source_path}")
                    folder_states[source_path][folder] = {
                        "state": current_state, "stable_count": 0}
                else:
                    previous_state = folder_states[source_path][folder][
                        "state"]
                    # Check if folder state is unchanged
                    if current_state == previous_state:
                        folder_states[source_path][folder]["stable_count"] += 1
                        logging.debug(
                            f"Folder '{folder}' stability check passed: {folder_states[source_path][folder]['stable_count']} of {stable_checks}")
                    else:
                        # Reset state and stability count if folder content changed
                        folder_states[source_path][folder][
                            "state"] = current_state
                        folder_states[source_path][folder]["stable_count"] = 0
                        logging.info(
                            f"Folder '{folder}' content changed, resetting stability checks.")

                # Process folder if it is stable for the required number of checks
                if folder_states[source_path][folder][
                    "stable_count"] >= stable_checks:
                    match = re.match(r'^([^-]*)-([^-]*)$', folder)
                    if match:
                        project_name, user_name = match.groups()
                        logging.info(
                            f"Stable folder ready for processing: {folder} (Project: {project_name}, User: {user_name})")

                        destination_path = find_actual_destination(
                            destination_bases, project_name)
                        if not destination_path:
                            logging.warning(
                                f"No matching destination found for project '{project_name}'. Skipping folder.")
                            continue

                        destination_path = os.path.join(destination_path,
                                                        *ingest_prefix.split(
                                                            "/"), user_name)
                        os.makedirs(destination_path, exist_ok=True)

                        if destination_path not in destination_cache:
                            destination_cache[
                                destination_path] = get_folder_state(
                                destination_path)

                        logging.info(
                            f"Copying files from {folder_path} to {destination_path}...")
                        copied = copy_folder(folder_path, destination_path,
                                             destination_cache[
                                                 destination_path])

                        if copied:
                            destination_cache[
                                destination_path] = get_folder_state(
                                destination_path)
                            if compare_folders(folder_path, destination_path,
                                               destination_cache[
                                                   destination_path]):
                                logging.info(
                                    f"Folder {folder} successfully copied to {destination_path}.")
                                seen_folders.add(folder)
                            else:
                                logging.warning(
                                    f"Folder {folder} was not fully copied to {destination_path}.")
                    else:
                        logging.warning(
                            f"Skipping folder '{folder}' as it does not match the expected format.")

            # Remove folders that no longer exist in the source
            folder_states[source_path] = {folder: state for folder, state in
                                          folder_states.get(source_path,
                                                            {}).items() if
                                          folder in current_folders}
            # Save folder states after processing
            save_folder_states(source_path, folder_states)

        except Exception as e:
            logging.error(f"Error during monitoring: {e}", exc_info=True)

        logging.debug(f"Sleeping for {check_interval} seconds.")
        time.sleep(check_interval)


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
    return state


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
                shutil.copy2(source_file,
                             dest_file)  # Efficient file copy with metadata
                logging.debug(f"Copied file: {source_file} -> {dest_file}")
                copied = True

    return copied


def compare_folders(source, destination, destination_state):
    """
    Compares the contents of source and destination folders using cached state.

    :param source: Path to the source folder.
    :param destination: Path to the destination folder.
    :param destination_state: Cached state of the destination folder.
    :return: True if folders match, False otherwise.
    """
    source_state = get_folder_state(source)
    return source_state == destination_state


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
    parser.add_argument("--check_interval", type=int, default=10,
                        help="Time interval between checks (in seconds). Default is 10.")
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

