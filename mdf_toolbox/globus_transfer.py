from copy import deepcopy
from datetime import datetime
import os
import time

import globus_sdk

from mdf_toolbox.filesystem import posixify_path


# *************************************************
# * Globus Transfer utilities
# *************************************************

DEFAULT_INTERVAL = 1 * 60  # 1 minute, in seconds
DEFAULT_INACTIVITY_TIME = 1 * 24 * 60 * 60  # 1 day, in seconds


def custom_transfer(transfer_client, source_ep, dest_ep, path_list, interval=DEFAULT_INTERVAL,
                    inactivity_time=DEFAULT_INACTIVITY_TIME):
    """Perform a Globus Transfer.

    Arguments:
        transfer_client (TransferClient): An authenticated Transfer client.
        source_ep (str): The source Globus Endpoint ID.
        dest_ep (str): The destination Globus Endpoint ID.
        path_list (list of tuple of 2 str): A list of tuples containing the paths to transfer as
                ``(source, destination)``.

                **Example**::

                    [("/source/files/file.dat", "/dest/mydocs/doc.dat"),
                     ("/source/all_reports/", "/dest/reports/")]

        interval (int): Number of seconds to wait before polling Transfer status.
                Minimum ``1``. **Default**: ``DEFAULT_INTERVAL``.
        inactivity_time (int): Number of seconds a Transfer is allowed to go without progress
                before being cancelled. **Default**: ``DEFAULT_INACTIVITY_TIME``.

    Yields:
        dict: An error from the transfer, or (last) a success status.

    Accepts via ``.send()``:
        *bool*: ``True``: Continue the Transfer
                ``False``: Cancel the Transfer
                **Default**: ``True``
    """
    # Ensure paths are POSIX
    for i, path in enumerate(path_list):
        path_list[i] = (posixify_path(path[0]), posixify_path(path[1]))

    # TODO: (LW) Handle transfers with huge number of files
    # If a TransferData object is too large, Globus might timeout
    #   before it can be completely uploaded.
    # So, we need to be able to check the size of the TD object and, if need be, send it early.
    if interval < 1:
        interval = 1
    deadline = datetime.utcfromtimestamp(int(time.time()) + inactivity_time)
    tdata = globus_sdk.TransferData(transfer_client, source_ep, dest_ep,
                                    deadline=deadline, verify_checksum=True)
    for item in path_list:
        # Check if source path is directory or missing
        source_res = globus_check_directory(transfer_client, source_ep, item[0],
                                            allow_missing=False)
        if not source_res["success"]:
            raise globus_sdk.GlobusError(source_res["error"])
        source_is_dir = source_res["is_dir"]

        # Check if dest path is directory
        dest_res = globus_check_directory(transfer_client, dest_ep, item[1], allow_missing=True)
        if not dest_res["success"]:
            raise globus_sdk.GlobusError(dest_res["error"])
        dest_exists = dest_res["exists"]
        dest_is_dir = dest_res["is_dir"]

        # Transfer dir
        if source_is_dir and (not dest_exists or dest_is_dir):
            tdata.add_item(item[0], item[1], recursive=True)
        # Transfer non-dir
        elif not source_is_dir and (not dest_exists or not dest_is_dir):
            tdata.add_item(item[0], item[1])
        # Transfer non-dir into dir
        # TODO: Is this logic user-friendly or is it surprising?
        # Take non-dir source filename, Transfer to dest dir+filename
        elif not source_is_dir and (dest_exists and dest_is_dir):
            new_dest = os.path.join(item[1], os.path.basename(item[0]))
            tdata.add_item(item[0], new_dest)
        # Malformed - Cannot transfer dir into non-dir
        else:
            raise globus_sdk.GlobusError("Cannot transfer a directory into a file: "
                                         + str(item))

    res = transfer_client.submit_transfer(tdata)
    if res["code"] != "Accepted":
        raise globus_sdk.GlobusError("Failed to transfer files: Transfer " + res["code"])

    error_timestamps = set()
    # while Transfer is active
    while not transfer_client.task_wait(res["task_id"],
                                        timeout=interval, polling_interval=interval):
        for event in transfer_client.task_event_list(res["task_id"]):
            # Only process error events that have not been presented to the user
            # Events do not have UUIDs, so if there are multiple simultaneous errors
            #   only the last (chronologically) error will be processed
            if event["is_error"] and event["time"] not in error_timestamps:
                error_timestamps.add(event["time"])
                ret_event = deepcopy(event.data)
                # yield value should always have success: bool
                ret_event["success"] = False
                ret_event["finished"] = False
                # User can cancel Transfer with .send(False)
                cont = yield ret_event
                if cont is False:
                    transfer_client.cancel_task(res["task_id"])
                    # Wait until Transfer is no longer active after cancellation
                    while not transfer_client.task_wait(res["task_id"],
                                                        timeout=1, polling_interval=1):
                        pass
                    break
            # If progress has been made, move deadline forward
            elif event["code"] == "PROGRESS":
                new_deadline = datetime.utcfromtimestamp(int(time.time()) + inactivity_time)
                new_doc = {
                    "DATA_TYPE": "task",
                    "deadline": str(new_deadline)
                }
                transfer_client.update_task(res["task_id"], new_doc)
    # Transfer is no longer active; now check if succeeded
    task = transfer_client.get_task(res["task_id"]).data
    task["success"] = (task["status"] == "SUCCEEDED")
    task["finished"] = True
    yield task


def globus_check_directory(transfer_client, endpoint, path, allow_missing=False):
    """Check if a path on a Globus Endpoint is a directory or file.

    Arguments:
        transfer_client (TransferClient): An authenticated Transfer client.
        endpoint (str): The Endpoint ID.
        path (str): The path on the Endpoint to check.
        allow_missing (bool): When ``True``, the path not being found is not an error.
                When ``False``, the path must exist for the check to succeed.
                **Default**: ``False``.

    Returns:
        dict: Results of the check.
            success (bool): ``True`` if the check was able to be performed.
            error (str): The error encountered, if any.
            exists (bool): ``True`` iff the path exists on the endpoint.
                    If ``allow_missing`` is ``False``, ``exists`` being ``False`` is an error.
            is_dir (bool): ``True`` iff the path is confirmed to lead to a directory.
            is_file (bool): ``True`` iff the path is confirmed to lead to a file.

    Note: ``is_dir`` and ``is_file`` will both be ``False`` if ``allow_missing`` is ``True``
            and ``exists`` is ``False``.
    """
    # is_dir has three states:
    #   True (dir confirmed), implies exists is True
    #   False (file confirmed), implies exists is True
    #   None (no information)
    is_dir = None
    # exists can be:
    #   True (exists, type unknown), also implied if is_dir is not None
    #   False (confirmed missing)
    #   None (no information)
    exists = None
    # error can either be None (no error) or a string (error)
    # The presence of an error implies success is False
    error = None

    # Try operation_ls on the path, which gives actionable info about the path
    try:
        transfer_client.operation_ls(endpoint, path=path)
        is_dir = True
    except globus_sdk.TransferAPIError as e:
        # If error indicates path exists but is not dir, is not dir
        if e.code == "ExternalError.DirListingFailed.NotDirectory":
            is_dir = False
        # Too many files in dir indicates is dir
        elif e.code == "ExternalError.DirListingFailed.SizeLimit":
            is_dir = True
        # Not found must be logged
        elif e.code == "ClientError.NotFound":
            exists = False
        # Else, retry on parent dir (some other error occurred)
        else:
            try:
                parent, item_name = os.path.split(path)
                parent_ls = transfer_client.operation_ls(parent, path=parent)
                type_list = [x["type"] for x in parent_ls["DATA"] if x["name"] == item_name]
                # If item_name not found in list, other error occurred on missing path
                # Odd, but still a missing path
                if len(type_list) < 1:
                    exists = False
                # If multiple hits, panic (shouldn't occur, but...)
                # Technically possible in GDrive connector?
                elif len(type_list) > 1:
                    raise globus_sdk.GlobusError("Multiple items with name '{}' in path '{}'"
                                                 "on endpoint '{}'"
                                                 .format(item_name, parent, endpoint))
                else:
                    # Otherwise we have exactly one hit - the correct node
                    item_type = type_list[0]
                    if item_type == "dir":
                        is_dir = True
                    elif item_type == "file":
                        is_dir = False
                    # If not file or dir, but does exist, log an error
                    else:
                        exists = True
                        error = ("Path '{}' leads to a '{}', not a file or directory"
                                 .format(path, item_type))
            except globus_sdk.TransferAPIError as e:
                # Size limit means we can't figure out this path
                if e.code == "ExternalError.DirListingFailed.SizeLimit":
                    error = ("Unable to check type of path '{}': Parent directory too large"
                             .format(path))
                # Not found must be logged (not sure this branch is possible)
                elif e.code == "ClientError.NotFound":
                    exists = False
                # Can't handle other error on parent
                else:
                    error = str(e)

    # If path must exist but doesn't, set error
    if exists is False and allow_missing is False:
        error = "Path '{}' not found on endpoint '{}'".format(path, endpoint)

    return {
        "success": (error is None),
        "error": error,
        "exists": (exists or is_dir is not None),
        "is_dir": (is_dir is True),
        "is_file": (is_dir is False)
    }


def quick_transfer(transfer_client, source_ep, dest_ep, path_list, interval=None, retries=10):
    """Perform a Globus Transfer and monitor for success.

    Arguments:
        transfer_client (TransferClient): An authenticated Transfer client.
        source_ep (str): The source Globus Endpoint ID.
        dest_ep (str): The destination Globus Endpoint ID.
        path_list (list of tuple of 2 str): A list of tuples containing the paths to transfer as
                ``(source, destination)``.

                **Example**::

                    [("/source/files/file.dat", "/dest/mydocs/doc.dat"),
                     ("/source/all_reports/", "/dest/reports/")]

        interval (int): Number of seconds to wait before polling Transfer status.
                Minimum ``1``.**Default**: ``DEFAULT_INTERVAL``.
        retries (int): The number of errors to tolerate before cancelling the task.
                Globus Transfer makes no distinction between hard errors
                (e.g. "permission denied") and soft errors
                (e.g. "endpoint [temporarily] too busy") so requiring retries is
                not uncommon for large Transfers.
                ``-1`` for infinite tries (Transfer still fails after a period of no activity).
                ``None`` is synonymous with ``0``.
                **Default**: ``10``.

    Returns:
        str: ID of the Globus Transfer.
    """
    if retries is None:
        retries = 0
    iterations = 0

    transfer = custom_transfer(transfer_client, source_ep, dest_ep, path_list)
    res = next(transfer)
    try:
        # Loop ends on StopIteration from generator exhaustion
        while True:
            if iterations < retries or retries == -1:
                res = transfer.send(True)
                iterations += 1
            else:
                res = transfer.send(False)
    except StopIteration:
        pass
    if res["success"]:
        error = "No error"
    else:
        error = "{}: {}".format(res.get("fatal_error", {}).get("code", "Error"),
                                res.get("fatal_error", {}).get("description", "Unknown"))
    return {
        "success": res["success"],
        "task_id": res["task_id"],
        "error": error
    }
