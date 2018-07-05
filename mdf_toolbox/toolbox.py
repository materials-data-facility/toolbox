from datetime import datetime
import json
import os
import requests
import shutil
import time

from globus_nexus_client import NexusClient
import globus_sdk
from globus_sdk.base import BaseClient
from globus_sdk.response import GlobusHTTPResponse


KNOWN_SCOPES = {
    "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
    "search": "urn:globus:auth:scope:search.api.globus.org:search",
    "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
    "data_mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all",
    "publish": ("https://auth.globus.org/scopes/"
                "ab24b500-37a2-4bad-ab66-d8232c18e6e5/publish_api"),
    "connect": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "mdf_connect": "https://auth.globus.org/scopes/c17f27bb-f200-486a-b785-2a25e82af505/connect",
    "petrel": "https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all",
    "groups": "urn:globus:auth:scope:nexus.api.globus.org:groups"
}
KNOWN_TOKEN_KEYS = {
    "transfer": "transfer.api.globus.org",
    "search": "search.api.globus.org",
    "search_ingest": "search.api.globus.org",
    "data_mdf": "data.materialsdatafacility.org",
    "publish": "publish.api.globus.org",
    "connect": "mdf_dataset_submission",
    "mdf_connect": "mdf_dataset_submission",
    "petrel": "petrel_https_server",
    "groups": "nexus.api.globus.org"
}
KNOWN_CLIENTS = {
    "transfer": globus_sdk.TransferClient,
    "search": globus_sdk.SearchClient,
    "search_ingest": globus_sdk.SearchClient,
    #  "publish": DataPublicationClient,  # Defined in this module, added to dict later
    #  "mdf_connect": MDFConnectClient,   # Defined in this module, added to dict later
    "groups": NexusClient
}
SEARCH_INDEX_UUIDS = {
    "mdf": "1a57bbe5-5272-477f-9d31-343b8258b7a5",
    "mdf-test": "5acded0c-a534-45af-84be-dcf042e36412",
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}
CONNECT_SERVICE_LOC = "https://api.materialsdatafacility.org"
CONNECT_DEV_LOC = "https://dev-api.materialsdatafacility.org"
CONNECT_CONVERT_ROUTE = "/convert"
CONNECT_STATUS_ROUTE = "/status/"
DEFAULT_INTERVAL = 1 * 60  # 1 minute, in seconds
DEFAULT_INACTIVITY_TIME = 1 * 24 * 60 * 60  # 1 day, in seconds


# *************************************************
# * Authentication utilities
# *************************************************

def login(credentials=None, app_name=None, services=None, client_id=None, make_clients=True,
          clear_old_tokens=False, **kwargs):
    """Login to Globus services

    Arguments:
    credentials (str or dict): A string filename, string JSON, or dictionary
                                   with credential and config information.
                               By default, looks in ~/mdf/credentials/globus_login.json.
                               Contains app_name, services, and client_id as described below.
    app_name (str): Name of script/client. This will form the name of the token cache file.
                    Default 'UNKNOWN'.
    services (list of str): Services to authenticate with.
                            Services are listed in AUTH_SCOPES.
                            Default [].
    client_id (str): The ID of the client, given when registered with Globus.
                     Default is the MDF Native Clients ID.
    make_clients (bool): If True, will make and return appropriate clients with generated tokens.
                         If False, will only return authorizers.
                         Default True.
    clear_old_tokens (bool): If True, delete old token file if it exists, forcing user to re-login.
                             If False, use existing token file if there is one.
                             Default False.

    Returns:
    dict: The clients and authorizers requested, indexed by service name.
          For example, if login() is told to auth with 'search'
            then the search client will be in the 'search' field.
          Note: Previously requested tokens (which are cached) will be returned alongside
            explicitly requested ones.
    """
    NATIVE_CLIENT_ID = "98bfc684-977f-4670-8669-71f8337688e4"
    DEFAULT_CRED_FILENAME = "globus_login.json"
    DEFAULT_CRED_PATH = os.path.expanduser("~/.mdf/credentials")

    def _get_tokens(client, scopes, app_name, force_refresh=False):
        token_path = os.path.join(DEFAULT_CRED_PATH, app_name + "_tokens.json")
        if force_refresh:
            if os.path.exists(token_path):
                os.remove(token_path)
        if os.path.exists(token_path):
            with open(token_path, "r") as tf:
                try:
                    tokens = json.load(tf)
                    # Check that requested scopes are present
                    # :all scopes should override any scopes with lesser permissions
                    # Some scopes are returned in multiples and should be separated
                    existing_scopes = []
                    for sc in [val["scope"] for val in tokens.values()]:
                        if " " in sc:
                            existing_scopes += sc.split(" ")
                        else:
                            existing_scopes.append(sc)
                    permissive_scopes = [scope.replace(":all", "")
                                         for scope in existing_scopes
                                         if scope.endswith(":all")]
                    missing_scopes = [scope for scope in scopes.split(" ")
                                      if scope not in existing_scopes
                                      and not any([scope.startswith(per_sc)
                                                   for per_sc in permissive_scopes])
                                      and not scope.strip() == ""]
                    # If some scopes are missing, regenerate tokens
                    # Get tokens for existing scopes and new scopes
                    if len(missing_scopes) > 0:
                        scopes = " ".join(existing_scopes + missing_scopes)
                        os.remove(token_path)
                except ValueError:
                    # Tokens corrupted
                    os.remove(token_path)
        if not os.path.exists(token_path):
            try:
                os.makedirs(DEFAULT_CRED_PATH)
            except (IOError, OSError):
                pass
            client.oauth2_start_flow(requested_scopes=scopes, refresh_tokens=True)
            authorize_url = client.oauth2_get_authorize_url()

            print("It looks like this is the first time you're accessing this service.",
                  "\nPlease log in to Globus at this link:\n", authorize_url)
            auth_code = input("Copy and paste the authorization code here: ").strip()

            # Handle 401s
            try:
                token_response = client.oauth2_exchange_code_for_tokens(auth_code)
            except globus_sdk.GlobusAPIError as e:
                if e.http_status == 401:
                    raise ValueError("\nSorry, that code isn't valid."
                                     " You can try again, or contact support.")
                else:
                    raise
            tokens = token_response.by_resource_server

            os.umask(0o077)
            with open(token_path, "w") as tf:
                json.dump(tokens, tf)
            print("Thanks! You're now logged in.")

        return tokens

    # If creds supplied in 'credentials', process
    if credentials:
        if type(credentials) is str:
            try:
                with open(credentials) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                try:
                    creds = json.loads(credentials)
                except ValueError:
                    raise ValueError("Credential string unreadable")
        elif type(credentials) is dict:
            creds = credentials
        else:
            try:
                with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                try:
                    with open(os.path.join(DEFAULT_CRED_PATH, DEFAULT_CRED_FILENAME)) as cred_file:
                        creds = json.load(cred_file)
                except IOError:
                    raise ValueError("Credentials/configuration must be passed as a "
                                     + "filename string, JSON string, or dictionary, "
                                     + "or provided in '"
                                     + DEFAULT_CRED_FILENAME
                                     + "' or '"
                                     + DEFAULT_CRED_PATH
                                     + "'.")
        app_name = creds.get("app_name")
        services = creds.get("services", services)
        client_id = creds.get("client_id")
    if not app_name:
        app_name = "UNKNOWN"
    if not services:
        services = []
    elif isinstance(services, str):
        services = [services]
    if not client_id:
        client_id = NATIVE_CLIENT_ID

    native_client = globus_sdk.NativeAppAuthClient(client_id, app_name=app_name)

    servs = []
    for serv in services:
        serv = serv.lower().strip()
        if type(serv) is str:
            servs += serv.split(" ")
        else:
            servs += list(serv)
    # Translate services into scopes as possible
    scopes = " ".join([KNOWN_SCOPES.get(sc, sc) for sc in servs])

    all_tokens = _get_tokens(native_client, scopes, app_name, force_refresh=clear_old_tokens)

    # Make authorizers with every returned token
    all_authorizers = {}
    for key, tokens in all_tokens.items():
        # TODO: Allow non-Refresh authorizers
        try:
            all_authorizers[key] = globus_sdk.RefreshTokenAuthorizer(tokens["refresh_token"],
                                                                     native_client)
        except KeyError:
            print("Error: Unable to retrieve tokens for '{}'.\n"
                  "You may need to delete your old tokens and retry.".format(key))
    returnables = {}
    # Populate clients and named services
    # Only translate back services - if user provides scope directly, don't translate back
    # ex. transfer => urn:transfer.globus.org:all => transfer,
    #     but urn:transfer.globus.org:all !=> transfer
    for service in servs:
        token_key = KNOWN_TOKEN_KEYS.get(service)
        # If the .by_resource_server key (token key) for the service was returned
        if token_key in all_authorizers.keys():
            # If there is an applicable client (all clients have known token key)
            # Pop from all_authorizers to remove from final return value
            if make_clients and KNOWN_CLIENTS.get(service):
                try:
                    returnables[service] = KNOWN_CLIENTS[service](
                                                authorizer=all_authorizers.pop(token_key))
                except globus_sdk.GlobusAPIError as e:
                    print("Error: Unable to create {} client: {}".format(service, e.message))
            # If no applicable client, just translate the key
            else:
                returnables[service] = all_authorizers.pop(token_key)
    # Add authorizers not associated with service to returnables
    returnables.update(all_authorizers)

    return returnables


def confidential_login(credentials=None, client_id=None, client_secret=None, services=None,
                       make_clients=True):
    """Login to Globus services as a confidential client (a client with its own login information).

    Arguments:
    credentials (str or dict): A string filename, string JSON, or dictionary
                                   with credential and config information.
                               By default, uses the DEFAULT_CRED_FILENAME and DEFAULT_CRED_PATH.
        Contains client_id, client_secret, and services as defined below.
    client_id (str): The ID of the client.
    client_secret (str): The client's secret for authentication.
    services (list of str): Services to authenticate with.
                            Services are listed in AUTH_SCOPES.
    make_clients (bool): If True, will make and return appropriate clients with generated tokens.
                         If False, will only return authorizers.
                         Default True.

    Returns:
    dict: The clients and authorizers requested, indexed by service name.
          For example, if confidential_login() is told to auth with 'search'
            then the search client will be in the 'search' field.
    """
    DEFAULT_CRED_FILENAME = "confidential_globus_login.json"
    DEFAULT_CRED_PATH = os.path.expanduser("~/.mdf/credentials")
    # Read credentials if supplied
    if credentials:
        if type(credentials) is str:
            try:
                with open(credentials) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                try:
                    creds = json.loads(credentials)
                except ValueError:
                    raise ValueError("Credentials unreadable or missing")
        elif type(credentials) is dict:
            creds = credentials
        else:
            try:
                with open(os.path.join(os.getcwd(), DEFAULT_CRED_FILENAME)) as cred_file:
                    creds = json.load(cred_file)
            except IOError:
                try:
                    with open(os.path.join(DEFAULT_CRED_PATH, DEFAULT_CRED_FILENAME)) as cred_file:
                        creds = json.load(cred_file)
                except IOError:
                    raise ValueError("Credentials/configuration must be passed as a "
                                     "filename string, JSON string, or dictionary, or provided "
                                     "in '{}' or '{}'.".format(DEFAULT_CRED_FILENAME,
                                                               DEFAULT_CRED_PATH))
        client_id = creds.get("client_id")
        client_secret = creds.get("client_secret")
        services = creds.get("services", services)
    if not client_id or not client_secret:
        raise ValueError("A client_id and client_secret are required.")
    if not services:
        services = []
    elif isinstance(services, str):
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

    # Make authorizers with every returned token
    all_authorizers = {}
    for scope in scopes:
        # TODO: Allow non-CC authorizers?
        try:
            all_authorizers[scope] = globus_sdk.ClientCredentialsAuthorizer(conf_client, scope)
        except Exception as e:
            print("Error: Cannot create authorizer for scope '{}' ({})".format(scope, str(e)))
    returnables = {}
    # Populate clients and named services
    # Only translate back services - if user provides scope directly, don't translate back
    # ex. transfer => urn:transfer.globus.org:all => transfer,
    #     but urn:transfer.globus.org:all !=> transfer
    for service in servs:
        token_key = KNOWN_SCOPES.get(service)
        # If the .by_resource_server key (token key) for the service was returned
        if token_key in all_authorizers.keys():
            # If there is an applicable client (all clients have known token key)
            # Pop from all_authorizers to remove from final return value
            if make_clients and KNOWN_CLIENTS.get(service):
                try:
                    returnables[service] = KNOWN_CLIENTS[service](
                                                authorizer=all_authorizers.pop(token_key))
                except globus_sdk.GlobusAPIError as e:
                    print("Error: Unable to create {} client: {}".format(service, e.message))
            # If no applicable client, just translate the key
            else:
                returnables[service] = all_authorizers.pop(token_key)
    # Add authorizers not associated with service to returnables
    returnables.update(all_authorizers)

    return returnables


def anonymous_login(services):
    """Initialize services without authenticating to Globus Auth.

    Arguments:
    services (str or list of str): The services to initialize clients for.
                                   Note that clients may have reduced functionality
                                   without authentication.

    Returns:
    dict: The clients requested, indexed by service name.
          For example, if anonymous_login() is told to auth with 'search'
            then the search client will be in the 'search' field.
    """
    if isinstance(services, str):
        services = [services]

    clients = {}
    # Initialize valid services
    for serv in services:
        try:
            clients[serv] = KNOWN_CLIENTS[serv]()
        except KeyError:  # No known client
            print("Error: No known client for '{}' service.".format(serv))
        except Exception:  # Other issue, probably auth
            print("Error: Unable to create client for '{}' service.\n"
                  "Anonymous access may not be allowed.".format(serv))

    return clients


# *************************************************
# * File utilities
# *************************************************

def uncompress_tree(root, delete_archives=False):
    """Uncompress all tar, zip, and gzip archives under a given directory.
    Archives will be extracted to a sibling directory named after the archive (minus extension).
    This process can be slow, depending on the number and size of archives.

    Arguments:
    root (str): The path to the starting (root) directory.
    delete_archives (bool): If True, will delete extracted archive files.
                            If False, will preserve archive files.
                            Default False.
    Returns:
    dict: Results.
        success (bool): If the extraction succeeded.
        num_extracted (int): Number of archives extracted.
    """
    num_extracted = 0
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
                else:
                    num_extracted += 1
                    # Add new dir to list of dirs to process
                    extract_dirs.append(extracted_files_dir)
                    if delete_archives:
                        os.remove(archive_path)
    return {
        "success": True,
        "num_extracted": num_extracted
    }


# *************************************************
# * Globus Search utilities
# *************************************************

def format_gmeta(data, acl=None, identifier=None):
    """Format input into GMeta format, suitable for ingesting into Globus Search.
    Format a dictionary into a GMetaEntry.
    Format a list of GMetaEntry into a GMetaList inside a GMetaIngest.

    Example usage:
        glist = []
        for document in all_my_documents:
            gmeta_entry = format_gmeta(document, ["public"], document["id"])
            glist.append(gmeta_entry)
        ingest_ready_document = format_gmeta(glist)

    Arguments:
    data (dict or list): The data to be formatted.
        If data is a dict, arguments acl and id1 are required.
        If data is a list, it must consist of GMetaEntry documents.
    acl (list of str): The list of Globus UUIDs allowed to view the document,
                       or the special value ["public"] to allow anyone access.
                       Required if data is a dict. Ignored if data is a list.
    identifier (str): A unique identifier for this document. If this value is not unique,
                      ingests into Globus Search may merge entries.
                      Required is data is a dict. Ignored if data is a list.

    Returns:
    dict (if data is dict): The data as a GMetaEntry.
    dict (if data is list): The data as a GMetaIngest.
    """
    if isinstance(data, dict):
        if acl is None or identifier is None:
            raise ValueError("acl and identifier are required when formatting a GMetaEntry.")
        if isinstance(acl, str):
            acl = [acl]
        return {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": identifier,
            "visible_to": acl,
            "content": data
            }

    elif isinstance(data, list):
        return {
            "@datatype": "GIngest",
            "@version": "2016-11-09",
            "ingest_type": "GMetaList",
            "ingest_data": {
                "@datatype": "GMetaList",
                "@version": "2016-11-09",
                "gmeta": data
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
    info (bool): If False, gmeta_pop will return a list of the results and discard the metadata.
                 If True, gmeta_pop will return a tuple containing the results list,
                    and other information about the query.
                 Default False.

    Returns:
    list (if info=False): The unwrapped results.
    tuple (if info=True): The unwrapped results, and a dictionary of query information.
    """
    if type(gmeta) is GlobusHTTPResponse:
        gmeta = json.loads(gmeta.text)
    elif type(gmeta) is str:
        gmeta = json.loads(gmeta)
    elif type(gmeta) is not dict:
        raise TypeError("gmeta must be dict, GlobusHTTPResponse, or JSON string")
    results = []
    for res in gmeta["gmeta"]:
        for con in res["content"]:
            results.append(con)
    if info:
        fyi = {
            "total_query_matches": gmeta["total"]
            }
        return results, fyi
    else:
        return results


def translate_index(index_name):
    """Translate a known Globus Search index into the index UUID.
    The UUID is the proper way to access indices, and will eventually be the only way.
    This method will not change names it cannot disambiguate.

    Arguments:
    index_name (str): The name of the index.

    Returns:
    str: The UUID of the index. If the index is not known and is not unambiguous,
            this will be the index_name unchanged.
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

def custom_transfer(transfer_client, source_ep, dest_ep, path_list,
                    interval=DEFAULT_INTERVAL, inactivity_time=DEFAULT_INACTIVITY_TIME):
    """Perform a Globus Transfer.

    Arguments:
    transfer_client (TransferClient): An authenticated Transfer client.
    source_ep (str): The source Globus Endpoint ID.
    dest_ep (str): The destination Globus Endpoint ID.
    path_list (list of tuple of 2 str): A list of tuples containing the paths to transfer as
                                        (source, destination).
        Directory paths must end in a slash, and file paths must not.
        Example: [("/source/files/file.dat", "/dest/mydocs/doc.dat"),
                  ("/source/all_reports/", "/dest/reports/")]
    interval (int): Number of seconds to wait before polling Transfer status.
                    Default DEFAULT_INTERVAL. Minimum 1.
    inactivity_time (int): Number of seconds a Transfer is allowed to go without progress
                           before being cancelled. Default DEFAULT_INACTIVITY_TIME.

    Yields:
    dict: An error from the transfer, or (last) a success status

    Accepts via .send():
    bool: True: Continue the Transfer
          False: Cancel the Transfer
          Default True
    """
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
        # Is not directory
        if item[0][-1] != "/" and item[1][-1] != "/":
            tdata.add_item(item[0], item[1])
        # Is directory
        elif item[0][-1] == "/" and item[1][-1] == "/":
            tdata.add_item(item[0], item[1], recursive=True)
        # Malformed
        else:
            raise globus_sdk.GlobusError("Cannot transfer file to directory or vice-versa: "
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


def quick_transfer(transfer_client, source_ep, dest_ep, path_list, interval=None, retries=10):
    """Perform a Globus Transfer and monitor for success.

    Arguments:
    transfer_client (TransferClient): An authenticated Transfer client.
    source_ep (str): The source Globus Endpoint ID.
    dest_ep (str): The destination Globus Endpoint ID.
    path_list (list of tuple of 2 str): A list of tuples containing the paths to transfer as
                                        (source, destination).
        Directory paths must end in a slash, and file paths must not.
        Example: [("/source/files/file.dat", "/dest/mydocs/doc.dat"),
                  ("/source/all_reports/", "/dest/reports/")]
    interval (int): Number of seconds to wait before polling Transfer status.
                    Default DEFAULT_INTERVAL. Minimum 1.
    retries (int): The number of errors to tolerate before cancelling the task.
                   Globus Transfer makes no distinction between
                   hard errors (e.g. "permission denied")
                   and soft errors (e.g. "endpoint [temporarily] too busy")
                   so requiring retries is not uncommon for large Transfers.
                   -1 for infinite tries (Transfer still fails after a period of no activity).
                   None is synonymous with 0.
                   Default 10.

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


def get_local_ep(transfer_client):
    """Discover the local Globus Connect Personal endpoint's ID, if possible.

    Arguments:
    transfer_client (TransferClient): An authenticated Transfer client.

    Returns:
    str: The local GCP EP ID if it was discovered.
    If the ID is not discovered, an exception will be raised.
        (globus_sdk.GlobusError unless the user cancels the search)
    """
    pgr_res = transfer_client.endpoint_search(filter_scope="my-endpoints")
    ep_candidates = pgr_res.data
    # Check number of candidates
    if len(ep_candidates) < 1:
        # Nothing found
        raise globus_sdk.GlobusError("Error: No local endpoints found")
    elif len(ep_candidates) == 1:
        # Exactly one candidate
        if not ep_candidates[0]["gcp_connected"]:
            # Is GCP, is not on
            raise globus_sdk.GlobusError("Error: Globus Connect is not running")
        else:
            # Is GCServer or GCP and connected
            return ep_candidates[0]["id"]
    else:
        # >1 found
        # Filter out disconnected GCP
        ep_connections = [candidate for candidate in ep_candidates
                          if candidate["gcp_connected"] is not False]
        # Recheck list
        if len(ep_connections) < 1:
            # Nothing found
            raise globus_sdk.GlobusError("Error: No local endpoints running")
        elif len(ep_connections) == 1:
            # Exactly one candidate
            if not ep_connections[0]["gcp_connected"]:
                # Is GCP, is not on
                raise globus_sdk.GlobusError("Error: Globus Connect is not active")
            else:
                # Is GCServer or GCP and connected
                return ep_connections[0]["id"]
        else:
            # Still >1 found
            # Prompt user
            print("Multiple endpoints found:")
            count = 0
            for ep in ep_connections:
                count += 1
                print(count, ": ", ep["display_name"], "\t", ep["id"])
            print("\nPlease choose the endpoint on this machine")
            ep_num = 0
            while ep_num == 0:
                usr_choice = input("Enter the number of the correct endpoint (-1 to cancel): ")
                try:
                    ep_choice = int(usr_choice)
                    if ep_choice == -1:
                        # User wants to quit
                        ep_num = -1
                    elif ep_choice in range(1, count+1):
                        # Valid selection
                        ep_num = ep_choice
                    else:
                        # Invalid number
                        print("Invalid selection")
                except Exception:
                    print("Invalid input")

            if ep_num == -1:
                print("Cancelling")
                raise SystemExit
            return ep_connections[ep_num-1]["id"]


# *************************************************
# * Misc utilities
# *************************************************

def dict_merge(base, addition):
    """Merge one dictionary with another, recursively.
    Fields present in addition will be added to base.
    No data in base is deleted or overwritten.

    Arguments:
    base (dict): The dictionary being added to.
    addition (dict): The dictionary with additional data.

    Returns:
    dict: The merged base.
    """
    if not isinstance(base, dict) or not isinstance(addition, dict):
        raise TypeError("dict_merge only works with dicts.")

    for key, value in addition.items():
        # If the value is a dict, need to merge those
        if isinstance(value, dict):
            base[key] = dict_merge(base.get(key, {}), value)
        # Otherwise, if the key is not in base, add it
        elif key not in base.keys():
            base[key] = value

    return base


# *************************************************
# * Clients
# *************************************************

class MDFConnectClient:
    """MDFConnect"""
    __app_name = "MDF_Connect_Client"
    __login_services = ["connect"]
    __allowed_authorizers = [
        globus_sdk.RefreshTokenAuthorizer,
        globus_sdk.ClientCredentialsAuthorizer,
        globus_sdk.NullAuthorizer
    ]

    def __init__(self, dc=None, mdf=None, mrr=None, custom=None,
                 data=None, index=None, services=None, test=False,
                 service_instance=None, authorizer=None):
        self.dc = dc or {}
        self.mdf = mdf or {}
        self.mrr = mrr or {}
        self.custom = custom or {}
        self.data = data or []
        self.index = index or {}
        self.services = services or {}
        self.test = test

        if service_instance == "prod" or service_instance is None:
            self.service_loc = CONNECT_SERVICE_LOC
        elif service_instance == "dev":
            self.service_loc = CONNECT_DEV_LOC
        else:
            self.service_loc = service_instance
        self.convert_route = CONNECT_CONVERT_ROUTE
        self.status_route = CONNECT_STATUS_ROUTE

        self.source_id = None

        if any([isinstance(authorizer, allowed) for allowed in self.__allowed_authorizers]):
            self.__authorizer = authorizer
        else:
            self.__authorizer = login({"app_name": self.__app_name,
                                       "services": self.__login_services}).get("connect")
        if not self.__authorizer:
            raise ValueError("Unable to authenticate")

    def create_dc_block(self, title, authors,
                        affiliations=None, publisher=None, publication_year=None,
                        resource_type=None,
                        description=None, dataset_doi=None, related_dois=None,
                        **kwargs):
        """Create your submission's dc block.

        Arguments:

        Required arguments:
        title (str or list of str): The title(s) of the dataset.
        authors (str or list of str): The author(s) of the dataset.
                                      Format must be one of:
                                        "Givenname Familyname"
                                        "Familyname, Givenname"
                                        "Familyname; Givenname"
                                      No additional commas or semicolons are permitted.

        Arguments with usable defaults:
        affiliations (str or list of str or list of list of str):
                      The affiliations of the authors, in the same order.
                      If a different number of affiliations are given,
                      all affiliations will be applied to all authors.
                      Multiple affiliations can be given as a list.
                      Default None for no affiliations for any author.
                      Examples:
                        authors = ["Fromnist, Alice", "Fromnist; Bob", "Cathy Multiples"]
                        # All authors are from NIST
                        affiliations = "NIST"
                        # All authors are from both NIST and UChicago
                        affiliations = ["NIST", "UChicago"]
                        # Alice and Bob are from NIST, Cathy is from NIST and UChicago
                        affliliations = ["NIST", "NIST", ["NIST", "UChicago"]]

                        # This is incorrect! If applying affiliations to all authors,
                        #   lists must not be nested.
                        affiliations = ["NIST", ["NIST", "UChicago"], "Argonne", "Oak Ridge"]
        publisher (str): The publisher of the dataset (not an associated paper). Default MDF.
        publication_year (int or str): The year of dataset publication. Default current year.
        resource_type (str): The type of resource. Except in unusual cases, this should be
                             "Dataset". Default "Dataset".

        Optional arguments:
        description (str): A description of the dataset. Default None for no description.
        dataset_doi (str): The DOI for this dataset (not an associated paper). Default None.
        related_dois (str or list of str): DOIs related to this dataset,
                                           not including the dataset's own DOI
                                           (for example, an associated paper's DOI).
                                           Default None.

        Additional keyword arguments:
            Any further keyword arguments will be added to the DataCite metadata (the dc block).
            These arguments should be valid DataCite, as listed in the MDF Connect documentation.
            This is completely optional.
        """
        # titles
        if not isinstance(title, list):
            title = [title]
        titles = [{"title": t} for t in title]

        # creators
        if not isinstance(authors, list):
            authors = [authors]
        if not affiliations:
            affiliations = []
        elif not isinstance(affiliations, list):
            affiliations = [affiliations]
        if not len(authors) == len(affiliations):
            affiliations = [affiliations] * len(authors)
        creators = []
        for auth, affs in zip(authors, affiliations):
            if auth.find(",") >= 0:
                family, given = auth.split(",", 1)
            elif auth.find(";") >= 0:
                family, given = auth.split(";", 1)
            elif auth.find(" ") >= 0:
                given, family = auth.split(" ", 1)
            else:
                given = auth
                family = ""
            if not isinstance(affs, list):
                affs = [affs]

            family = family.strip()
            given = given.strip()
            creator = {
                "creatorName": family + ", " + given,
                "familyName": family,
                "givenName": given
            }
            if affs:
                creator["affiliations"] = affs
            creators.append(creator)

        # publisher
        if not publisher:
            publisher = "Materials Data Facility"

        # publicationYear
        try:
            publication_year = str(int(publication_year))
        except (ValueError, TypeError):
            publication_year = str(datetime.now().year)

        # resourceType
        if not resource_type:
            resource_type = "Dataset"

        dc = {
            "titles": titles,
            "creators": creators,
            "publisher": publisher,
            "publicationYear": publication_year,
            "resourceType": {
                "resourceTypeGeneral": "Dataset",
                "resourceType": resource_type
            }
        }

        # descriptions
        if description:
            dc["descriptions"] = [{
                "description": description,
                "descriptionType": "Other"
            }]

        # identifier
        if dataset_doi:
            dc["identifier"] = {
                "identifier": dataset_doi,
                "identifierType": "DOI"
            }

        # relatedIdentifiers
        if related_dois:
            if not isinstance(related_dois, list):
                related_dois = [related_dois]
            dc["relatedIdentifiers"] = [{
                "relatedIdentifier": doi,
                "relatedIdentifierType": "DOI",
                "relationType": "IsPartOf"
            } for doi in related_dois]

        # misc
        if kwargs:
            dc = dict_merge(dc, kwargs)

        self.dc = dc

    def set_acl(self, acl):
        """Set the Access Control List for your dataset.

        Arguments:
        acl (str or list of str): The Globus UUIDs of users or groups that
                                  should be granted access to the dataset.
                                  The default is special keyword "public"
                                  that makes the dataset visible to everyone.
        """
        if not isinstance(acl, list):
            acl = [acl]
        self.mdf["acl"] = acl

    def clear_acl(self):
        """Reset the ACL of your dataset to the default value ["public"]."""
        self.mdf.pop("acl", None)

    def set_source_name(self, source_name):
        """Set the source name for your dataset.

        Arguments:
        source_name (str): The desired source name. Must be unique for new datasets.
                           Please note that your source name will be cleaned
                           when submitted to Connect,
                           so the actual source_name may differ from this value.
                           Additionally, the source_id (which is the source_name plus version)
                           is required to fetch the status of a submission.
                           .check_status() can handle this for you.
        """
        self.mdf["source_name"] = source_name

    def clear_source_name(self):
        """Remove a previously set source_name."""
        self.mdf.pop("source_name", None)

    def add_repositories(self, repositories):
        """Add repositories to your dataset.

        Arguments:
        repositories (str or list of str): The repository or repositories to add.
                                           If the repository is not known to MDF, it will
                                           be discarded.
                                           Additional repositories may be added automatically.
        """
        if not isinstance(repositories, list):
            repositories = [repositories]
        if not self.mdf.get("repositories"):
            self.mdf["repositories"] = repositories
        else:
            self.mdf["repositories"].extend(repositories)

    def clear_repositories(self):
        """Clear all added repositories from the submission."""
        self.mdf.pop("repositories", None)

    def create_mrr_block(self, mrr_data):
        """Create the mrr block for your dataset.
        Note that this helper will be more helpful in the future.

        Arguments:
        mrr_data (dict): The MRR schema-compliant metadata.
        """
        self.mrr = mrr_data

    def set_custom_block(self, custom_fields):
        """Set the __custom block for your dataset.

        Arguments:
        custom_fields (dict): Custom field-value pairs for your dataset.
        """
        self.custom = custom_fields

    def add_data(self, data_location):
        """Add a data location to your dataset.
        Note that this method is cumulative, so calls do not overwrite previous ones.

        Arguments:
        data_location (str or list of str): The location(s) of the data.
                                            These should be formatted with protocol.
                                            Examples:
                                                https://example.com/path/data.zip
                                                https://www.globus.org/app/transfer?...
                                                globus://endpoint123/path/data.out
        """
        if not isinstance(data_location, list):
            data_location = [data_location]
        self.data.extend(data_location)

    def clear_data(self):
        """Clear all data added so far to your dataset."""
        self.data = []

    def add_index(self, data_type, mapping, delimiter=None, na_values=None):
        """Add indexing instructions for your dataset.
        This method can be called multiple times for multiple data types,
        but multiple calls with the same data type will overwrite each other.

        Arguments:
        data_type (str): The type of data to apply to. Supported types are:
                         json
                         csv
                         yaml
                         xml
                         excel
        mapping (dict): The mapping of MDF fields to your data type's fields.
                        It is strongly recommended that you use "dot notation",
                        where nested JSON objects are represented with a period.
                        Examples:
                        {
                            "material.composition": "my_json.data.stuff.comp",
                            "dft.converged": "my_json.data.dft.abcd"
                        }
                        {
                            "material.composition": "csv_header_1",
                            "crystal_structure.space_group_number": "csv_header_2"
                        }
        delimiter (str): The character that delimits cells in a table.
                         Only applicable to tabular data.
                         Default comma.
        na_values (str or list of str): Values to treat as N/A (not applicable/available).
                                        Only applicable to tabular data.
                                        Default blank and space.
        """
        # TODO: Validation
        index = {
            "mapping": mapping
        }
        if delimiter is not None:
            index["delimiter"] = delimiter
        if na_values is not None:
            if not isinstance(na_values, list):
                na_values = [na_values]
            index["na_values"] = na_values

        self.index[data_type] = index

    def clear_index(self):
        """Clear all indexing instructions set so far."""
        self.index = {}

    def add_service(self, service, parameters=None):
        """Add a service for data submission.

        Arguments:
        service (str): The integrated service to submit your dataset to.
                       Connected services include:
                        globus_publish (publication with DOI minting)
                        citrine (industry-partnered machine-learning specialists)
        parameters (dict): Optional, service-specific parameters.
            For globus_publish:
                collection_id (int): The collection for submission. Overwrites collection_name.
                collection_name (str): The collection for submission.
            For citrine:
                public (bool): When true, will make data public. Otherwise, it is inaccessible.
        """
        if parameters is None:
            parameters = True
        self.services[service] = parameters

    def clear_services(self):
        """Clear all services added so far."""
        self.services = {}

    def set_test(self, test):
        """Set the test flag for this dataset.

        Arguments:
        test (bool): When False, the dataset will be processed normally.
                     When True, the dataset will be processed, but submitted to
                        test/sandbox/temporary resources instead of live resources.
                        This includes the mdf-test Search index and MDF Test Publish collection.
        """
        self.test = test

    def get_submission(self):
        """Fetch the current state of your submission.

        Returns:
        dict: Your submission.
        """
        submission = {
            "dc": self.dc,
            "data": self.data,
            "test": self.test
        }
        if self.mdf:
            submission["mdf"] = self.mdf
        if self.mrr:
            submission["mrr"] = self.mrr
        if self.custom:
            submission["__custom"] - self.custom
        if self.index:
            submission["index"] = self.index
        if self.services:
            submission["services"] = self.services
        return submission

    def reset_submission(self):
        """Completely clear all metadata from your submission.
        This action cannot be undone.
        The last submission's source_id will also be cleared. If you want to use check_status,
        you will be required to input the source_id manually.

        Returns:
        dict: The variables that are NOT cleared, including:
            test
            service_location
        """
        self.dc = {}
        self.mdf = {}
        self.mrr = {}
        self.custom = {}
        self.clear_data()
        self.clear_index()
        self.clear_services()
        self.source_id = None

        return {
            "test": self.test,
            "service_location": self.service_loc
        }

    def submit_dataset(self, test=False, resubmit=False, submission=None, reset=False):
        """Submit your dataset to MDF Connect for processing.

        Arguments:
        test (bool): Submit as a test dataset (a dry-run, see set_test()).
                     If you have called set_test() or otherwise specified test=True,
                     you do not need to use this argument.
        resubmit (bool): If you wish to submit this dataset again, set this to True.
                         If this is the first submission, leave this False.
        submission (dict): If you have assembled the Connect metadata yourself,
                           you can submit it here. This argument supersedes any data
                           set through other methods.
                           Default None, to use method-assembled data.
        reset (bool): If True, will clear the old submission. The test flag will be preserved.
                      IMPORTANT: The source_id of the submission will not be saved if
                                 this argument is True. check_status will require you to
                                 pass the source_id as an argument.
                      If False, the submission will be preserved.
                      Default False.

        Returns:
        str: The source_id of your dataset. This is also saved in self.source_id.
             The source_id is the source_name plus the version.
             In other words, source_name is unique to your dataset,
             and source_id is unique to your submission of the dataset.
        """
        # Ensure resubmit matches reality
        if not resubmit and self.source_id:
            print("You have already submitted this dataset.")
            return None
        elif resubmit and not self.source_id:
            print("You have not already submitted this dataset.")
            return None

        if not submission:
            submission = self.get_submission()

        # Check for required data
        if not submission["dc"] or not submission["data"]:
            print("You must populate the dc and data blocks before submission.")
            return None

        headers = {}
        self.__authorizer.set_authorization_header(headers)
        res = requests.post(self.service_loc+self.convert_route,
                            json=submission, headers=headers)
        try:
            json_res = res.json()
        except json.JSONDecodeError:
            print("Error decoding {} response: {}".format(res.status_code, res.content))
        else:
            if res.status_code < 300:
                self.source_id = json_res["source_id"]
            else:
                print("Error {} submitting dataset: {}".format(res.status_code, json_res))

        if not reset:
            return self.source_id
        else:
            source_id = self.source_id
            self.reset_submission()
            return source_id

    def check_status(self, source_id=None, raw=False):
        """Check the status of your submission.
        You may only check the status of your own submissions.

        Arguments:
        source_id (str): The source_id (source_name + version) of the submitted dataset.
                         Default self.source_id.
        raw (bool): When False, will print a nicely-formatted status summary.
                    When True, will return the full status result.
                    For direct human consumption, False is recommended. Default False.

        Returns:
        If raw is True, dict: The full status.
        """
        if not source_id and not self.source_id:
            print("Error: No dataset submitted")
            return None
        headers = {}
        self.__authorizer.set_authorization_header(headers)
        res = requests.get(self.service_loc+self.status_route+(source_id or self.source_id),
                           headers=headers)
        try:
            json_res = res.json()
        except json.JSONDecodeError:
            print("Error decoding {} response: {}".format(res.status_code, res.content))
        else:
            if res.status_code >= 300:
                print("Error {} fetching status: {}".format(res.status_code, json_res))
            elif raw:
                return json_res
            else:
                print("\n", json_res["status_message"], sep="")


class DataPublicationClient(BaseClient):
    """Publish data with Globus Publish."""

    def __init__(self, base_url="https://publish.globus.org/v1/api/", **kwargs):
        app_name = kwargs.pop('app_name', 'DataPublication Client v0.1')
        BaseClient.__init__(self, "datapublication", base_url=base_url,
                            app_name=app_name, **kwargs)
        self._headers['Content-Type'] = 'application/json'

    def list_schemas(self, **params):
        return self.get('schemas', params=params)

    def get_schema(self, schema_id, **params):
        return self.get('schemas/{}'.format(schema_id), params=params)

    def list_collections(self, **params):
        try:
            return self.get('collections', params=params)
        except Exception as e:
            print('FAIL: {}'.format(e))

    def list_datasets(self, collection_id, **params):
        return self.get('collections/{}/datasets'.format(collection_id),
                        params=params)

    def push_metadata(self, collection, metadata, **params):
        return self.post('collections/{}'.format(collection),
                         json_body=metadata, params=params)

    def get_dataset(self, dataset_id, **params):
        return self.get('datasets/{}'.format(dataset_id),
                        params=params)

    def get_submission(self, submission_id, **params):
        return self.get('submissions/{}'.format(submission_id),
                        params=params)

    def delete_submission(self, submission_id, **params):
        return self.delete('submissions/{}'.format(submission_id),
                           params=params)

    def complete_submission(self, submission_id, **params):
        return self.post('submissions/{}/submit'.format(submission_id),
                         params=params)

    def list_submissions(self, **params):
        return self.get('submissions', params=params)


# Add Toolbox clients to known clients
KNOWN_CLIENTS["publish"] = DataPublicationClient
KNOWN_CLIENTS["mdf_connect"] = MDFConnectClient
