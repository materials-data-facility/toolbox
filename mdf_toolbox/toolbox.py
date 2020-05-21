from collections.abc import Container, Iterable, Mapping
from copy import deepcopy
from datetime import datetime
import json
import os
import shutil
import time

from fair_research_login import NativeClient
from globus_nexus_client import NexusClient
import globus_sdk
from globus_sdk.response import GlobusHTTPResponse
import jsonschema


KNOWN_SCOPES = {
    "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
    "search": "urn:globus:auth:scope:search.api.globus.org:search",
    "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
    "data_mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all",
    "mdf_connect": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "petrel": "https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all",
    "groups": "urn:globus:auth:scope:nexus.api.globus.org:groups",
    "dlhub": "https://auth.globus.org/scopes/81fc4156-a623-47f2-93ad-7184118226ba/auth"
}
KNOWN_TOKEN_KEYS = {
    "transfer": "transfer.api.globus.org",
    "search": "search.api.globus.org",
    "search_ingest": "search.api.globus.org",
    "data_mdf": "data.materialsdatafacility.org",
    "mdf_connect": "mdf_dataset_submission",
    "petrel": "petrel_https_server",
    "groups": "nexus.api.globus.org",
    "dlhub": "dlhub_org"
}
KNOWN_CLIENTS = {
    KNOWN_SCOPES["transfer"]: globus_sdk.TransferClient,
    "transfer": globus_sdk.TransferClient,
    KNOWN_SCOPES["search"]: globus_sdk.SearchClient,
    "search": globus_sdk.SearchClient,
    KNOWN_SCOPES["search_ingest"]: globus_sdk.SearchClient,
    "search_ingest": globus_sdk.SearchClient,
    KNOWN_SCOPES["groups"]: NexusClient,
    "groups": NexusClient
}
SEARCH_INDEX_UUIDS = {
    "mdf": "1a57bbe5-5272-477f-9d31-343b8258b7a5",
    "mdf-test": "5acded0c-a534-45af-84be-dcf042e36412",
    "mdf-dev": "aeccc263-f083-45f5-ab1d-08ee702b3384",
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}
DEFAULT_APP_NAME = "UNNAMED_APP"
DEFAULT_CLIENT_ID = "984464e2-90ab-433d-8145-ac0215d26c8e"
DEFAULT_INTERVAL = 1 * 60  # 1 minute, in seconds
DEFAULT_INACTIVITY_TIME = 1 * 24 * 60 * 60  # 1 day, in seconds
STD_TIMEOUT = 5 * 60  # 5 minutes


# *************************************************
# * Authentication utilities
# *************************************************

def login(services, make_clients=True, clear_old_tokens=False, **kwargs):
    """Log in to Globus services.

    Arguments:
        services (list of str): The service names or scopes to authenticate to.
        make_clients (bool): If ``True``, will make and return appropriate clients with
                generated tokens. If ``False``, will only return authorizers.
                **Default**: ``True``.
        clear_old_tokens (bool): Force a login flow, even if loaded tokens are valid.
                Same effect as ``force``. If one of these is ``True``, the effect triggers
                **Default**: ``False``.

    Keyword Arguments:
        app_name (str): Name of the app/script/client. Used for the named grant during consent,
                and the local server browser page by default.
                **Default**: ``'UNKNOWN_APP'``.
        client_id (str): The ID of the client registered with Globus at
                https://developers.globus.org
                **Default**: The MDF Native Clients ID.
        no_local_server (bool): Disable spinning up a local server to automatically
                copy-paste the auth code. THIS IS REQUIRED if you are on a remote server.
                When used locally with no_local_server=False, the domain is localhost with
                a randomly chosen open port number.
                **Default**: ``False``.
        no_browser (bool): Do not automatically open the browser for the Globus Auth URL.
                Display the URL instead and let the user navigate to that location manually.
                **Default**: ``False``.
        refresh_tokens (bool): Use Globus Refresh Tokens to extend login time.
                **Default**: ``True``.
        force (bool): Force a login flow, even if loaded tokens are valid.
                Same effect as ``clear_old_tokens``. If one of these is ``True``, the effect
                triggers. **Default**: ``False``.

    Returns:
        dict: The clients and authorizers requested, indexed by service name.
                For example, if ``login()`` is told to auth with ``'search'``
                then the search client will be in the ``'search'`` field.
    """
    if isinstance(services, str):
        services = [services]
    # Set up arg defaults
    app_name = kwargs.get("app_name") or DEFAULT_APP_NAME
    client_id = kwargs.get("client_id") or DEFAULT_CLIENT_ID

    native_client = NativeClient(client_id=client_id, app_name=app_name)

    # Translate known services into scopes, existing scopes are cleaned
    servs = []
    for serv in services:
        serv = serv.lower().strip()
        if type(serv) is str:
            servs += serv.split(" ")
        else:
            servs += list(serv)
    scopes = [KNOWN_SCOPES.get(sc, sc) for sc in servs]

    native_client.login(requested_scopes=scopes,
                        no_local_server=kwargs.get("no_local_server", False),
                        no_browser=kwargs.get("no_browser", False),
                        refresh_tokens=kwargs.get("refresh_tokens", True),
                        force=clear_old_tokens or kwargs.get("force", False))

    all_authorizers = native_client.get_authorizers_by_scope(requested_scopes=scopes)
    returnables = {}
    # Process authorizers (rename keys to originals, make clients)
    for scope, auth in all_authorizers.items():
        # User specified known_scope name and not scope directly
        if scope not in servs:
            try:
                key = [k for k, v in KNOWN_SCOPES.items() if scope == v][0]
            except IndexError:  # Not a known scope(?), fallback to scope as key
                key = scope
        # User specified scope directly
        else:
            key = scope

        # User wants clients and client supported
        if make_clients and scope in KNOWN_CLIENTS.keys():
            returnables[key] = KNOWN_CLIENTS[scope](authorizer=auth, http_timeout=STD_TIMEOUT)
        # Returning authorizer only
        else:
            returnables[key] = auth

    return returnables


def confidential_login(services, client_id, client_secret, make_clients=True):
    """Log in to Globus services as a confidential client
    (a client with its own login information, i.e. NOT a human's account).

    Arguments:
        services (list of str): Services to authenticate with.
        client_id (str): The ID of the client.
        client_secret (str): The client's secret for authentication.
        make_clients (bool): If ``True``, will make and return appropriate clients
                with generated tokens.
                If ``False``, will only return authorizers.
                **Default**: ``True``.

    Returns:
        dict: The clients and authorizers requested, indexed by service name.
    """
    if isinstance(services, str):
        services = [services]

    conf_client = globus_sdk.ConfidentialAppAuthClient(client_id, client_secret)
    servs = []
    for serv in services:
        serv = serv.lower().strip()
        if type(serv) is str:
            servs += serv.split(" ")
        else:
            servs += list(serv)
    # Translate services into scopes as possible
    scopes = [KNOWN_SCOPES.get(sc, sc) for sc in servs]

    # Make authorizers for each scope requested
    all_authorizers = {}
    for scope in scopes:
        # TODO: Allow non-CC authorizers?
        try:
            all_authorizers[scope] = globus_sdk.ClientCredentialsAuthorizer(conf_client, scope)
        except Exception as e:
            print("Error: Cannot create authorizer for scope '{}' ({})".format(scope, str(e)))

    returnables = {}
    # Process authorizers (rename keys to originals, make clients)
    for scope, auth in all_authorizers.items():
        # User specified known_scope name and not scope directly
        if scope not in servs:
            try:
                key = [k for k, v in KNOWN_SCOPES.items() if scope == v][0]
            except IndexError:  # Not a known scope(?), fallback to scope as key
                key = scope
        # User specified scope directly
        else:
            key = scope

        # User wants clients and client supported
        if make_clients and scope in KNOWN_CLIENTS.keys():
            returnables[key] = KNOWN_CLIENTS[scope](authorizer=auth, http_timeout=STD_TIMEOUT)
        # Returning authorizer only
        else:
            returnables[key] = auth

    return returnables


def anonymous_login(services):
    """Initialize service clients without authenticating to Globus Auth.

    Note:
        Clients may have reduced functionality without authentication.

    Arguments:
        services (str or list of str): The services to initialize clients for.

    Returns:
        dict: The clients requested, indexed by service name.
    """
    if isinstance(services, str):
        services = [services]

    clients = {}
    # Initialize valid services
    for serv in services:
        try:
            clients[serv] = KNOWN_CLIENTS[serv](http_timeout=STD_TIMEOUT)
        except KeyError:  # No known client
            print("Error: No known client for '{}' service.".format(serv))
        except Exception:  # Other issue, probably auth
            print("Error: Unable to create client for '{}' service.\n"
                  "Anonymous access may not be allowed.".format(serv))

    return clients


def logout(app_name=None, client_id=None):
    """Revoke and delete all saved tokens for the app.

    Arguments:
        app_name (str): Name of the app/script/client.
                **Default**: ``'UNKNOWN_APP'``.
        client_id (str): The ID of the client.
                **Default**: The MDF Native Clients ID.
    """
    if not app_name:
        app_name = DEFAULT_APP_NAME
    if not client_id:
        client_id = DEFAULT_CLIENT_ID
    NativeClient(app_name=app_name, client_id=client_id).logout()


# *************************************************
# * File utilities
# *************************************************

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


# *************************************************
# * Globus Search utilities
# *************************************************

def get_globus_id_type(uuid):
    """**Not implemented**
    Determine the type of resource a Globus UUID identifies.
    This utility is not comprehensive.

    Arguments:
        uuid (str): A Globus UUID.

    Returns:
        str: The type of resource identified. Types this utility can identify:

                * ``identity``: A user's identity in Globus Auth
                * ``group``: A Globus Group
                * ``endpoint``: A Globus Transfer Endpoint

                This utility cannot detect other types of UUID.
                If the UUID is not one of the above types, or is invalid, the return
                value will be ``unknown``.
    """
    # TODO: Actually figure out if this is possible without
    #       serious Auth issues - how to get identity/group/etc. info?
    raise NotImplementedError("get_globus_id_type currently not functional")


def format_gmeta(data, acl=None, identifier=None):
    """Format input into GMeta format, suitable for ingesting into Globus Search.
    Formats a dictionary into a GMetaEntry.
    Formats a list of GMetaEntry into a GMetaList inside a GMetaIngest.
    The data suppied is copied with ``copy.deepcopy()`` so the original objects
    may be reused or deleted as needed.

    **Example usage**::

        glist = []
        for document in all_my_documents:
            gmeta_entry = format_gmeta(document, ["public"], document["id"])
            glist.append(gmeta_entry)
        ingest_ready_document = format_gmeta(glist)

    Arguments:
        data (dict or list): The data to be formatted.
                If data is a dict, arguments ``acl`` and ``identifier`` are required.
                If data is a list, it must consist of GMetaEntry documents.
        acl (list of str): The list of Globus UUIDs allowed to view the document,
                or the special value ``["public"]`` to allow anyone access.
                Required if data is a dict. Ignored if data is a list.
                Will be formatted into URNs if required.
        identifier (str): A unique identifier for this document. If this value is not unique,
                ingests into Globus Search may merge entries.
                Required is data is a dict. Ignored if data is a list.

    Returns:
        dict (if ``data`` is ``dict``): The data as a GMetaEntry.
        dict (if ``data`` is ``list``): The data as a GMetaIngest.
    """
    if isinstance(data, dict):
        if acl is None or identifier is None:
            raise ValueError("acl and identifier are required when formatting a GMetaEntry.")
        if isinstance(acl, str):
            acl = [acl]
        # "Correctly" format ACL entries into URNs
        prefixed_acl = []
        for uuid in acl:
            # If entry is not special value "public" and is not a URN, make URN
            # It is not known what the type of UUID is, so use both
            # This solution is known to be hacky
            if uuid != "public" and not uuid.lower().startswith("urn:"):
                prefixed_acl.append("urn:globus:auth:identity:"+uuid.lower())
                prefixed_acl.append("urn:globus:groups:id:"+uuid.lower())
            # Otherwise, no modification
            else:
                prefixed_acl.append(uuid)

        return {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": deepcopy(identifier),
            "visible_to": prefixed_acl,
            "content": deepcopy(data)
        }

    elif isinstance(data, list):
        # No known versions other than "2016-11-09"
        try:
            version = deepcopy(data[0]["@version"])
        except Exception:
            version = "2016-11-09"
        return {
            "@datatype": "GIngest",
            "@version": version,
            "ingest_type": "GMetaList",
            "ingest_data": {
                "@datatype": "GMetaList",
                "@version": version,
                "gmeta": deepcopy(data)
            }
        }

    else:
        raise TypeError("Cannot format '" + str(type(data)) + "' into GMeta.")


def gmeta_pop(gmeta, info=False):
    """Remove GMeta wrapping from a Globus Search result.
    This function can be called on the raw GlobusHTTPResponse that Search returns,
    or a string or dictionary representation of it.

    Arguments:
        gmeta (dict, str, or GlobusHTTPResponse): The Globus Search result to unwrap.
        info (bool): If ``False``, will return a list of the results
                and discard the metadata. If ``True``, will return a tuple containing
                the results list, and other information about the query.
                **Default**: ``False``.

    Returns:
        list (if ``info=False``): The unwrapped results.
        tuple (if ``info=True``): The unwrapped results, and a dictionary of query information.
    """
    if type(gmeta) is GlobusHTTPResponse:
        gmeta = json.loads(gmeta.text)
    elif type(gmeta) is str:
        gmeta = json.loads(gmeta)
    elif type(gmeta) is not dict:
        raise TypeError("gmeta must be dict, GlobusHTTPResponse, or JSON string")
    results = []
    for res in gmeta["gmeta"]:
        # version 2017-09-01
        for con in res.get("content", []):
            results.append(con)
        # version 2019-08-27
        for ent in res.get("entries", []):
            results.append(ent["content"])
    if info:
        fyi = {
            "total_query_matches": gmeta.get("total")
            }
        return results, fyi
    else:
        return results


def translate_index(index_name):
    """Translate a known Globus Search index into the index UUID.
    The UUID is the proper way to access indices, and will eventually be the only way.
    This method will return names it cannot disambiguate.

    Arguments:
        index_name (str): The name of the index.

    Returns:
        str: The UUID of the index. If the index is not known and is not unambiguous,
                this will be the ``index_name`` unchanged instead.
    """
    uuid = SEARCH_INDEX_UUIDS.get(index_name.strip().lower())
    if not uuid:
        try:
            index_info = globus_sdk.SearchClient().get_index(index_name).data
            if not isinstance(index_info, dict):
                raise ValueError("Multiple UUIDs possible")
            uuid = index_info.get("id", index_name)
        except Exception:
            uuid = index_name
    return uuid


# *************************************************
# * Globus Transfer utilities
# *************************************************

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


def custom_transfer(transfer_client, source_ep, dest_ep, path_list, interval=DEFAULT_INTERVAL,
                    inactivity_time=DEFAULT_INACTIVITY_TIME, notify=True):
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
        notify (bool): When ``True``, trigger a notification email from Globus to the user when
                the Transfer succeeds or fails. When ``False``, disable the notification.
                **Default**: ``True``.

    Yields:
        dict: An error from the transfer, or (last) a success status.

    Accepts via ``.send()``:
        *bool*: ``True``: Continue the Transfer
                ``False``: Cancel the Transfer
                **Default**: ``True``
    """
    # TODO: (LW) Handle transfers with huge number of files
    # If a TransferData object is too large, Globus might timeout
    #   before it can be completely uploaded.
    # So, we need to be able to check the size of the TD object and, if need be, send it early.
    if interval < 1:
        interval = 1
    deadline = datetime.utcfromtimestamp(int(time.time()) + inactivity_time)
    tdata = globus_sdk.TransferData(transfer_client, source_ep, dest_ep,
                                    deadline=deadline, verify_checksum=True,
                                    notify_on_succeeded=notify, notify_on_failed=notify,
                                    notify_on_inactive=notify)
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
                ret_event = event.data
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


def quick_transfer(transfer_client, source_ep, dest_ep, path_list, interval=None, retries=10,
                   notify=True):
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
        notify (bool): When ``True``, trigger a notification email from Globus to the user when
                the Transfer succeeds or fails. When ``False``, disable the notification.
                **Default**: ``True``.

    Returns:
        str: ID of the Globus Transfer.
    """
    if retries is None:
        retries = 0
    iterations = 0

    transfer = custom_transfer(transfer_client, source_ep, dest_ep, path_list, notify=notify)
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


def get_local_ep(*args, **kwargs):
    """
    Warning:
        DEPRECATED: Use ``globus_sdk.LocalGlobusConnectPersonal().endpoint_id`` instead.
    """
    if kwargs.get("warn", True):
        raise DeprecationWarning("'get_local_ep()' has been deprecated in favor of "
                                 "'globus_sdk.LocalGlobusConnectPersonal().endpoint_id'. "
                                 "To override, pass in 'warn=False'.")
    else:
        import warnings
        warnings.warn("'get_local_ep()' has been deprecated in favor of "
                      "'globus_sdk.LocalGlobusConnectPersonal().endpoint_id'.")
    return globus_sdk.LocalGlobusConnectPersonal().endpoint_id


# *************************************************
# * JSON/JSONSchema/dict utilities
# *************************************************

def dict_merge(base, addition, append_lists=False):
    """Merge one dictionary with another, recursively.
    Fields present in addition will be added to base if not present or merged
    if both values are dictionaries or lists (with append_lists=True). If
    the values are different data types, the value in addition will be discarded.
    No data from base is deleted or overwritten.
    This function does not modify either dictionary.
    Dictionaries inside of other container types (list, etc.) are not merged,
    as the rules for merging would be ambiguous.
    If values from base and addition are of differing types, the value in
    addition is discarded.

    This utility could be expanded to merge Mapping and Container types in the future,
    but currently works only with dict and list.

    Arguments:
        base (dict): The dictionary being added to.
        addition (dict): The dictionary with additional data.
        append_lists (bool): When ``True``, fields present in base and addition
                that are lists will also be merged. Extra values from addition
                will be appended to the list in base.

    Returns:
        dict: The merged base.
    """
    if not isinstance(base, dict) or not isinstance(addition, dict):
        raise TypeError("dict_merge only works with dicts.")

    new_base = deepcopy(base)
    for key, value in addition.items():
        # Simplest case: Key not in base, so add value to base
        if key not in new_base.keys():
            new_base[key] = value
        # If the value is a dict, and base's value is also a dict, merge
        # If there is a type disagreement, merging cannot and should not happen
        if isinstance(value, dict) and isinstance(new_base[key], dict):
            new_base[key] = dict_merge(new_base[key], value)
        # If value is a list, lists should be merged, and base is compatible
        elif append_lists and isinstance(value, list) and isinstance(new_base[key], list):
            new_list = deepcopy(new_base[key])
            [new_list.append(item) for item in value if item not in new_list]
            new_base[key] = new_list
        # If none of these trigger, discard value from addition implicitly

    return new_base


def insensitive_comparison(item1, item2, type_insensitive=False, string_insensitive=False):
    """Compare two items without regard to order.

    The following rules are used to determine equivalence:
        * Items that are not of the same type can be equivalent only when ``type_insensitive=True``.
        * Mapping objects are equal iff the keys in each item exist in both items and have
          the same value (with the same ``insensitive_comparison``).
        * Other containers except for strings are equal iff every element in each item exists
          in both items (duplicate items must be present the same number of times).
        * Containers must be ``Iterable`` to be compared in this way.
        * Non-containers are equivalent if the equality operator returns ``True``.
        * Strings are treated as non-containers when ``string_insensitive=False``,
          and are treated as containers when ``string_insensitive=True``. When treated as
          containers, each (case-insensitive) character is treated as an element and
          whitespace is ignored.
        * If the items are in different categories above, they are never equivalent,
          even when ``type_insensitive=True``.

    Arguments:
        item1 (any): The first item to compare.
        item2 (any): The second item to compare.
        type_insensitive (bool): When ``True``, items of a different type are not automatically
                unequivalent. When ``False``, items must be the same type to be equivalent.
                **Default**: ``False``.
        string_insensitive (bool): When ``True``, strings are treated as containers, with each
                character being one element in the container.
                When ``False``, strings are treated as non-containers and compared directly.
                **Default**: ``False``.

    Returns:
        bool: ``True`` iff the two items are equivalent (see above).
                ``False`` otherwise.
    """
    # If type-sensitive, check types
    if not type_insensitive and type(item1) != type(item2):
        return False

    # Handle Mapping objects (dict)
    if isinstance(item1, Mapping):
        # Second item must be Mapping
        if not isinstance(item2, Mapping):
            return False
        # Items must have the same number of elements
        if not len(item1) == len(item2):
            return False
        # Keys must be the same
        if not insensitive_comparison(list(item1.keys()), list(item2.keys()),
                                      type_insensitive=True):
            return False
        # Each key's value must be the same
        # We can just check item1.items because the keys are the same
        for key, val in item1.items():
            if not insensitive_comparison(item1[key], item2[key],
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive):
                return False
        # Keys and values are the same
        return True
    # Handle strings
    elif isinstance(item1, str):
        # Second item must be string
        if not isinstance(item2, str):
            return False
        # Items must have the same number of elements (except string_insensitive)
        if not len(item1) == len(item2) and not string_insensitive:
            return False
        # If we're insensitive to case, spaces, and order, compare characters
        if string_insensitive:
            # If the string is one character long, skip additional comparison
            if len(item1) <= 1:
                return item1.lower() == item2.lower()
            # Make strings into containers (lists) and discard whitespace
            item1_list = [c for c in item1.lower() if not c.isspace()]
            item2_list = [c for c in item2.lower() if not c.isspace()]
            # The insensitive args shouldn't matter, but they're here just in case
            return insensitive_comparison(item1_list, item2_list,
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive)
        # Otherwise, case and order matter
        else:
            return item1 == item2
    # Handle other Iterable Containers
    elif isinstance(item1, Container) and isinstance(item1, Iterable):
        # Second item must be an Iterable Container
        if not isinstance(item2, Container) or not isinstance(item2, Iterable):
            return False
        # Items must have the same number of elements
        if not len(item1) == len(item2):
            return False
        # Every element in item1 must be in item2, and vice-versa
        # Painfully slow, but unavoidable for deep comparison
        # Each match in item1 removes the corresponding element from item2_copy
        # If they're the same, item2_copy should be empty at the end,
        #   unless a .remove() failed, in which case we have to re-match using item2
        item2_copy = list(deepcopy(item2))
        remove_failed = False
        for elem in item1:
            matched = False
            # Try every element
            for candidate in item2:
                # If comparison succeeds, flag a match, remove match from copy, and dump out
                if insensitive_comparison(elem, candidate,
                                          type_insensitive=type_insensitive,
                                          string_insensitive=string_insensitive):
                    matched = True
                    try:
                        item2_copy.remove(candidate)
                    except ValueError:  # list.remove(x): x not in list
                        remove_failed = True
                    break
            # One failure indicates unequivalence
            if not matched:
                return False
        # If all removes succeeded, we can shortcut checking all item2 elements in item1
        if not remove_failed:
            # If the Containers are equivalent, all elements in item2_copy should be removed
            # Otherwise
            return len(item2_copy) == 0
        # If something failed, we have to verify all of item2
        # We can't assume item2 != item1, because removal is comparative
        else:
            for elem in item2:
                matched = False
                # Try every element
                for candidate in item1:
                    # If comparison succeeds, flag a match, remove match from copy, and dump out
                    if insensitive_comparison(elem, candidate,
                                              type_insensitive=type_insensitive,
                                              string_insensitive=string_insensitive):
                        matched = True
                        break
                # One failure indicates unequivalence
                if not matched:
                    return False
            # All elements have a match
            return True
    # Handle otherwise unhandled type (catchall)
    else:
        return item1 == item2


def expand_jsonschema(schema, base_uri=None, definitions=None, resolver=None):
    """Expand references in a JSONSchema and return the dereferenced schema.
    Note:
        This function only dereferences simple ``$ref`` values. It does not
        dereference ``$ref`` values that are sufficiently complex.
        This tool is not exhaustive for all valid JSONSchemas.

    Arguments:
        schema (dict): The JSONSchema to dereference.
        base_uri (str): The base URI to the schema files, or a local path to the schema files.
                Required if ``resolver`` is not supplied (``base_uri`` is preferable).
        definitions (dict): Referenced definitions to start. Fully optional.
                **Default:** ``None``, to automatically populate definitions.
        resolver (jsonschema.RefResolver): The RefResolver to use in resolving ``$ref``
                values. Generally should not be set by users.
                **Default:** ``None``.

    Returns:
        dict: The dereferenced schema.
    """
    if not isinstance(schema, dict):
        return schema  # No-op on non-dict

    if resolver is None and base_uri is None:
        raise ValueError("base_uri is a required argument.")
    # Create RefResolver
    elif resolver is None:
        if os.path.exists(base_uri):
            base_uri = "{}{}{}".format(
                                    "file://" if not base_uri.startswith("file://") else "",
                                    os.path.abspath(base_uri),
                                    "/" if base_uri.endswith("/") else "")
        resolver = jsonschema.RefResolver(base_uri, None)

    if definitions is None:
        definitions = {}
    # Save schema's definitions
    # Could results in duplicate definitions, which has no effect
    if schema.get("definitions"):
        definitions = dict_merge(schema["definitions"], definitions)
        definitions = expand_jsonschema(definitions, definitions=definitions, resolver=resolver)
    while "$ref" in json.dumps(schema):
        new_schema = {}
        for key, val in schema.items():
            if key == "$ref":
                # $ref is supposed to take precedence, and effectively overwrite
                # other keys present, so we can make new_schema exactly the $ref value
                filename, intra_path = val.split("#")
                intra_parts = [x for x in intra_path.split("/") if x]
                # Filename ref refers to external file - resolve with RefResolver
                if filename:
                    ref_schema = resolver.resolve(filename)[1]
                    '''
                    with open(os.path.join(base_path, filename)) as schema_file:
                        ref_schema = json.load(schema_file)
                    '''
                    if ref_schema.get("definitions"):
                        definitions = dict_merge(ref_schema["definitions"], definitions)
                        definitions = expand_jsonschema(definitions, base_uri, definitions)
                    for path_part in intra_parts:
                        ref_schema = ref_schema[path_part]
                    # new_schema[intra_parts[-1]] = ref_schema
                    new_schema = ref_schema
                # Other refs should be in definitions block
                else:
                    if intra_parts[0] != "definitions" or len(intra_parts) != 2:
                        raise ValueError("Invalid/complex $ref: {}".format(intra_parts))
                    # new_schema[intra_parts[-1]] = definitions.get(intra_parts[1], "NONE")
                    new_schema = definitions.get(intra_parts[1], None)
                    if new_schema is None:
                        raise ValueError("Definition missing: {}".format(intra_parts))
            else:
                new_schema[key] = expand_jsonschema(val, definitions=definitions,
                                                    resolver=resolver)
        schema = new_schema
    return schema


def condense_jsonschema(schema, include_containers=True, list_items=True):
    """Condense a JSONSchema into a dict of dot-notated data fields and data types.
    This strips out all of the JSONSchema directives, like ``required`` and
    ``additionalProperties``, leaving only the fields that could actually be found in valid data.

    Caution:
        This tool is not exhaustive, and will not work correctly on all JSONSchemas.
        In particular, schemas with objects nested in arrays will not be handled correctly,
        and data fields that do not have a listed ``type`` will be skipped.
        Additionally, ``$ref`` elements WILL NOT be expanded. Use ``expand_jsonschema()`` on your
        schema first if you want references expanded.

    Arguments:
        schema (dict): The JSONSchema to condense. ``$ref`` elements will not be expanded.
        include_containers (bool): Should containers (dicts/objects, lists/arrays) be listed
                separately from their fields?
                **Default**: ``True``, which will list containers.
        list_items (bool): Should the field ``items`` be included in the data fields?
                ``items`` denotes an array of things, and it not directly a data field,
                but the output can be confusing without it.
                **Default**: ``True``.

    Returns:
        dict: The list of data fields, in dot notation, and the associated data type
                (when specified), as `data_field: data_type`.
    """
    data_fields = {}
    for field, value in flatten_json(schema, True).items():
        # TODO: Make this logic more robust (and less hacky).
        #       Generally works for MDF purposes; will explode on complex JSONSchemas.
        if field.endswith("type") and (include_containers
                                       or (value != "object" and value != "array")):
            clean_field = field.replace("properties.", "").replace(".type", "")
            if not list_items:
                clean_field = clean_field.replace(".items", "")
            data_fields[clean_field] = str(value)
    return data_fields


def prettify_jsonschema(root, **kwargs):
    """Prettify a JSONSchema. Pretty-yield instead of pretty-print.

    Caution:
            This utility is not robust! It is intended to work only with
            a subset of common JSONSchema patterns (mostly for MDF schemas)
            and does not correctly prettify all valid JSONSchemas.
            Use with caution.

    Arguments:
        root (dict): The schema to prettify.

    Keyword Arguments:
        num_indent_spaces (int): The number of spaces to consider one indentation level.
                **Default:** ``4``
        bullet (bool or str): Will prepend the character given as a bullet to properties.
                When ``True``, will use a dash. When ``False``, will not use any bullets.
                **Default:** ``True``
        _nest_level (int): A variable to track the number of iterations this recursive
                functions has gone through. Affects indentation level. It is not
                necessary nor advised to set this argument.
                **Default:** ``0``

    Yields:
        str: Lines of the JSONschema. To print the JSONSchema, just print each line.
             Stylistic newlines are included as empty strings. These can be ignored
             if a more compact style is preferred.
    """
    indent = " " * kwargs.get("num_indent_spaces", 4)
    if kwargs.get("bullet", True) is True:
        bullet = "- "
    else:
        bullet = kwargs.get("bullet") or ""
    _nest_level = kwargs.pop("_nest_level", 0)
    # root should always be dict, but if not just yield it
    if not isinstance(root, dict):
        yield "{}{}".format(indent*_nest_level, root)
        return

    # If "properties" is a field in root, display that instead of root's fields
    # Don't change _nest_level; we're skipping this level
    if "properties" in root.keys():
        yield from prettify_jsonschema(root["properties"], _nest_level=_nest_level, **kwargs)
        if root.get("required"):
            yield "{}Required: {}".format(indent*_nest_level, root["required"])
        yield ""  # Newline
    # Otherwise display the actual properties
    else:
        for field, val in root.items():
            try:
                # Non-dict should just be yielded
                if not isinstance(val, dict):
                    yield "{}{}: {}".format(indent*_nest_level, field, val)
                    continue

                # Treat arrays differently - nesting is one level deeper
                if val.get("items"):
                    # Base information (field, type, desc)
                    yield ("{}{}{} ({} of {}): {}"
                           .format(indent*_nest_level, bullet, field, val.get("type", "any type"),
                                   val["items"].get("type", "any type"),
                                   val.get("description",
                                           val["items"].get("description", "No description"))))
                    # List item limits
                    if val.get("minItems") and val.get("maxItems"):
                        if val["minItems"] == val["maxItems"]:
                            yield ("{}{}Must have exactly {} item(s)"
                                   .format(indent*_nest_level, " "*len(bullet), val["minItems"]))
                        else:
                            yield ("{}{}Must have between {}-{} items"
                                   .format(indent*_nest_level, " "*len(bullet), val["minItems"],
                                           val["maxItems"]))
                    elif val.get("minItems"):
                        yield ("{}{}Must have at least {} item(s)"
                               .format(indent*_nest_level, " "*len(bullet), val["minItems"]))
                    elif val.get("maxItems"):
                        yield ("{}{}Must have at most {} item(s)"
                               .format(indent*_nest_level, " "*len(bullet), val["maxItems"]))
                    # Recurse through properties
                    if val["items"].get("properties"):
                        yield from prettify_jsonschema(val["items"]["properties"],
                                                       _nest_level=_nest_level+1, **kwargs)
                    # List required properties
                    if val["items"].get("required"):
                        yield ("{}Required: {}"
                               .format(indent*(_nest_level+1), val["items"]["required"]))
                    yield ""  # Newline
                else:
                    # Base information (field, type, desc)
                    yield ("{}{}{} ({}): {}"
                           .format(indent*_nest_level, bullet, field, val.get("type", "any type"),
                                   val.get("description", "No description")))
                    # Recurse through properties
                    if val.get("properties"):
                        yield from prettify_jsonschema(val["properties"],
                                                       _nest_level=_nest_level+1, **kwargs)
                    # List required properties
                    if val.get("required"):
                        yield "{}Required: {}".format(indent*(_nest_level+1), val["required"])
                    yield ""  # Newline

            except Exception as e:
                yield ("{}Error: Unable to prettify information for field '{}'! ({})"
                       .format(indent*_nest_level, field, e))


def prettify_json(root, **kwargs):
    """Prettify a JSON object or list. Pretty-yield instead of pretty-print.

    Arguments:
        root (dict): The JSON to prettify.

    Keyword Arguments:
        num_indent_spaces (int): The number of spaces to consider one indentation level.
                **Default:** ``4``
        inline_singles (bool): When ``True``, will give non-container values inline
                for dictionary keys (e.g. "key: value"). When ``False``, will
                give non-container values on a separate line, like container values.
                **Default:** ``True``
        bullet (bool or str): Will prepend the character given as a bullet to properties.
                When ``True``, will use a dash. When ``False``, will not use any bullets.
                **Default:** ``True``
        _nest_level (int): A variable to track the number of iterations this recursive
                functions has gone through. Affects indentation level. It is not
                necessary nor advised to set this argument.
                **Default:** ``0``

    Yields:
        str: Lines of the prettified JSON, which can be directly printed if desired.
             Stylistic newlines are included as empty strings. These can be ignored
             if a more compact style is preferred.
    """
    indent = " " * kwargs.get("num_indent_spaces", 4)
    inline = kwargs.get("inline_singles", True)
    if kwargs.get("bullet", True) is True:
        bullet = "- "
    else:
        bullet = kwargs.get("bullet") or ""
    _nest_level = kwargs.pop("_nest_level", 0)
    if not root and root is not False:
        root = "None"

    # Prettify key/value pair
    if isinstance(root, dict):
        for k, v in root.items():
            # Containers and non-inline values should be recursively prettified
            if not inline or isinstance(v, dict) or isinstance(v, list):
                # Indent/bullet + key name
                yield "{}{}{}:".format(indent*_nest_level, bullet, k)
                # Value prettified with additional indent
                yield from prettify_json(v, _nest_level=_nest_level+1, **kwargs)
            # Otherwise, can prettify inline
            else:
                pretty_value = next(prettify_json(v, bullet=False, _nest_level=0))
                yield "{}{}{}: {}".format(indent*_nest_level, bullet, k, pretty_value)
            yield ""  # Newline
    # Prettify each item
    elif isinstance(root, list):
        for item in root:
            # Prettify values
            # No additional indent - nothing at top-level
            yield from prettify_json(item, _nest_level=_nest_level, **kwargs)
    # Just yield item
    else:
        yield "{}{}{}".format(indent*_nest_level, bullet, root)


def translate_json(source_doc, mapping, na_values=None, require_all=False):
    """Translate a JSON document (as a dictionary) from one schema to another.

    Note:
        Only JSON documents (and therefore datatypes permitted in JSON documents)
        are supported by this tool.

    Arguments:
        source_doc (dict): The source JSON document to translate.
        mapping (dict): The mapping of destination_fields: source_fields, in
                dot notation (where nested dicts/JSON objects are represented with a period).
                Missing fields are ignored.

                Examples::

                    {
                        "new_schema.some_field": "old_schema.stuff.old_fieldname"
                    }
                    {
                        "new_doc.organized.new_fieldname": "old.disordered.vaguename"
                    }
        na_values (list): Values to treat as N/A (not applicable/available).
                N/A values will be ignored and not copied.
                **Default:** ``None`` (no N/A values).
        require_all (bool): Must every value in the mapping be found? **Default:** ``False``.
                It is advised to leave this false unless the translated document depends
                on every key's value being present. Even so, it is advised to use
                JSONSchema validation instead.

    Returns:
        dict: The translated JSON document.
    """
    if na_values is None:
        na_values = []
    elif not isinstance(na_values, list):
        na_values = [na_values]

    # Flatten source_doc - will match keys easier
    flat_source = flatten_json(source_doc)
    # For each (dest, source) pair, attempt to fetch source's value to add to dest
    dest_doc = {}
    for dest_path, source_path in flatten_json(mapping).items():
        try:
            value = flat_source[source_path]
            # Check that the value is valid to translate, including contained values
            if isinstance(value, list):
                while any([na in value for na in na_values]):
                    [value.remove(na) for na in na_values if na in value]
            if value not in na_values and value != []:
                # Determine path to add
                fields = dest_path.split(".")
                last_field = fields.pop()
                current_field = dest_doc
                # Create all missing fields
                for field in fields:
                    if current_field.get(field) is None:
                        current_field[field] = {}
                    current_field = current_field[field]
                # Add value to end
                current_field[last_field] = value

        # KeyError indicates missing value - only panic if no missing values are allowed
        except KeyError as e:
            if require_all:
                raise KeyError("Required key '{}' not found during translation of JSON "
                               "document:\n{}".format(source_path, source_doc)) from e
    return dest_doc


def flatten_json(unflat_json, flatten_lists=True):
    """Flatten a JSON document into dot notation, where nested dicts are represented with a period.

    Arguments:
        unflat_json (dict): The JSON to flatten.
        flatten_lists (bool): Should the lists be flattened? **Default:** ``True``.
                Lists are flattened by merging contained dictionaries,
                and flattening those. Terminal values (non-container types)
                are added to a list and set at the terminal value for the path.
                When this is ``False``, lists are treated as terminal values and not flattened.

    Returns:
        dict: The JSON, flattened into dot notation in a dictionary.
                If a non-container value was supplied to flatten (e.g. a string)
                the value will be returned unchanged instead.

    Warning:
        Mixing container and non-container types in a list is not recommended.
        (e.g. [{"key": "val"}, "other_val"])
        If a list mixes types in this way, the non-container values MAY be listed
        under the field "flatten_undefined".

    Examples::

        {
            "key1": {
                "key2": "value"
            }
        }
        turns into
        {
            "key1.key2": value
        }


        {
            "key1": {
                "key2": [{
                    "key3": "foo",
                    "key4": "bar"
                }, {
                    "key3": "baz"
                }]
            }
        }
        with flatten_lists=True, turns into
        {
            "key1.key2.key3": ["foo", "baz"],
            "key1.key2.key4": "bar"
        }
    """
    flat_json = {}
    # Dict flattens by keys
    if isinstance(unflat_json, dict):
        for key, val in unflat_json.items():
            flat_val = flatten_json(val, flatten_lists=flatten_lists)
            # flat_val is dict to add to flat_json
            if isinstance(flat_val, dict):
                for subkey, subval in flat_val.items():
                    if subkey != "flatten_undefined":
                        flat_json[key+"."+subkey] = subval
                    # "flatten_unknown" is from mixed-type lists (container and non-container)
                    # Attempt to fix. This is not guaranteed; recommend not mixing types
                    else:
                        flat_json[key] = subval
            # flat_val is a terminal value (anything besides dict)
            else:
                flat_json[key] = flat_val

    # List flattens by values inside
    elif flatten_lists and isinstance(unflat_json, list):
        # Dict of flat keys processed so far
        partial_flats = {}
        # List of terminal values
        terminals = []
        for val in unflat_json:
            flat_val = flatten_json(val, flatten_lists=flatten_lists)
            # flat_val is dict, need to appropriately merge
            if isinstance(flat_val, dict):
                for subkey, subval in flat_val.items():
                    # If subkey is duplicate, add values to list
                    if subkey in partial_flats.keys():
                        # Create list if not already
                        if type(partial_flats[subkey]) is not list:
                            partial_flats[subkey] = [partial_flats[subkey], subval]
                        else:
                            partial_flats[subkey].append(subval)
                    # If subkey not duplicate, just add
                    else:
                        partial_flats[subkey] = subval
            # flat_val is a terminal value (anything besides dict)
            # Lists should be merged into terminals
            elif isinstance(flat_val, list):
                terminals.extend(flat_val)
            # Non-containers just appended to terminals
            else:
                terminals.append(flat_val)

        # Clean up for returning
        # If only one of partial_flats and terminals is populated, return that,
        # but if neither are flattened return an empty dict (partial_flats)
        # partial_flats is all contained dicts, flattened
        if not terminals:
            flat_json = partial_flats
        # terminals is all contained terminal values (flat by definition)
        elif terminals and not partial_flats:
            # If only one value in terminals, just return it
            if len(terminals) == 1:
                terminals = terminals[0]
            flat_json = terminals
        # Otherwise, add in sentinel field "flatten_undefined"
        # This case only occurs when a non-container type is mixed with a container type
        # in a list (e.g. [{"key": "val"}, "other_val"]) and is removed at an earlier
        # recursion depth if possible
        else:
            if len(terminals) == 1:
                terminals = terminals[0]
            partial_flats["flatten_undefined"] = terminals
            flat_json = partial_flats

    # Not container; cannot flatten
    else:
        flat_json = unflat_json
    return flat_json
