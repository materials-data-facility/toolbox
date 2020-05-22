"""Utility functions

These functions are intended for internal use by mdf_toolbox
"""
from pathlib import PureWindowsPath
import re


def rectify_path(path: str) -> str:
    """Ensure that a path is ready for use with Globus.

    Globus requires that paths are in POSIX format.
    Windows paths should be converted to POSIX style,
    where the "Drive" is listed as the
    first folder (e.g., ``/c/Users/globus_user/``).

    Args:
        path (str): Input path
    Returns:
        (str) Rectified path
    """

    is_windows = re.match('[A-Z]:\\\\', path) is not None
    if is_windows:
        ppath = PureWindowsPath(path)
        return f'/{ppath.drive[:1].lower()}{ppath.as_posix()[2:]}'
    return path  # Nothing to do for POSIX paths
