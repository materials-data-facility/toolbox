import os
from pathlib import PureWindowsPath
import re
import shutil


# *************************************************
# * Filesystem utilities
# *************************************************

def posixify_path(path: str) -> str:
    """Ensure that a path is in POSIX format.

    Windows paths are converted to POSIX style,
    where the "Drive" is listed as the
    first folder (e.g., ``/c/Users/globus_user/``).

    Arguments:
        path (str): Input path

    Returns:
        str: Rectified path
    """
    is_windows = re.match('[A-Z]:\\\\', path) is not None
    if is_windows:
        ppath = PureWindowsPath(path)
        return '/{0}{1}'.format(ppath.drive[:1].lower(), ppath.as_posix()[2:])
    return path  # Nothing to do for POSIX paths


def uncompress_tree(root, delete_archives=False):
    """Uncompress all tar, zip, and gzip archives under a given directory.
    Archives will be extracted to a sibling directory named after the archive (minus extension).
    This process can be slow, depending on the number and size of archives.

    Arguments:
        root (str): The path to the starting (root) directory.
        delete_archives (bool): If ``True``, will delete extracted archive files.
                                If ``False``, will preserve archive files.
                                **Default**: ``False``.

    Returns:
        dict: Results of the operation.
            * **success** (*bool*) - If the extraction succeeded.
            * **num_extracted** (*int*) - Number of archives extracted.
            * **files_errored** (*list of str*) - The files that threw an unexpected
                exception when extracted.
    """
    num_extracted = 0
    error_files = []
    # Start list of dirs to extract with root
    # Later, add newly-created dirs with extracted files, because os.walk will miss them
    extract_dirs = [os.path.abspath(os.path.expanduser(root))]
    while len(extract_dirs) > 0:
        for path, dirs, files in os.walk(extract_dirs.pop()):
            for filename in files:
                try:
                    # Extract my_archive.tar to sibling dir my_archive
                    archive_path = os.path.join(path, filename)
                    extracted_files_dir = os.path.join(path, os.path.splitext(filename)[0])
                    shutil.unpack_archive(archive_path, extracted_files_dir)
                except shutil.ReadError:
                    # ReadError means is not an (extractable) archive
                    pass
                except Exception:
                    error_files.append(os.path.join(path, filename))
                else:
                    num_extracted += 1
                    # Add new dir to list of dirs to process
                    extract_dirs.append(extracted_files_dir)
                    if delete_archives:
                        os.remove(archive_path)
    return {
        "success": True,
        "num_extracted": num_extracted,
        "files_errored": error_files
    }
