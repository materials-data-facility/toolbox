from collections.abc import Container, Iterable, Mapping
from copy import deepcopy
from datetime import datetime
import errno
import json
import os
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
    "groups": "urn:globus:auth:scope:nexus.api.globus.org:groups",
    "dlhub": "https://auth.globus.org/scopes/81fc4156-a623-47f2-93ad-7184118226ba/auth"
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
    "groups": "nexus.api.globus.org",
    "dlhub": "dlhub_org"
}
KNOWN_CLIENTS = {
    "transfer": globus_sdk.TransferClient,
    "search": globus_sdk.SearchClient,
    "search_ingest": globus_sdk.SearchClient,
    #  "publish": _DataPublicationClient,  # Defined in this module, added to dict later
    "groups": NexusClient
}
SEARCH_INDEX_UUIDS = {
    "mdf": "1a57bbe5-5272-477f-9d31-343b8258b7a5",
    "mdf-test": "5acded0c-a534-45af-84be-dcf042e36412",
    "mdf-dev": "aeccc263-f083-45f5-ab1d-08ee702b3384",
    "mdf-publish": "921907c6-4314-468b-a226-24edf5366cd9",
    "mdf-publish-test": "05a5f8cc-e8cf-4ced-8426-d87cba7c0be3",
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}
DEFAULT_INTERVAL = 1 * 60  # 1 minute, in seconds
DEFAULT_INACTIVITY_TIME = 1 * 24 * 60 * 60  # 1 day, in seconds
STD_TIMEOUT = 5 * 60  # 5 minutes
DEFAULT_CRED_PATH = os.path.expanduser("~/.mdf/credentials")


# *************************************************
# * Authentication utilities
# *************************************************

def login(credentials=None, app_name=None, services=None, client_id=None, make_clients=True,
          clear_old_tokens=False, token_dir=DEFAULT_CRED_PATH, **kwargs):
    """Log in to Globus services

    Arguments:
        credentials (str or dict): A string filename, string JSON, or dictionary
                with credential and config information.
                By default, looks in ``~/mdf/credentials/globus_login.json``.
                Contains ``app_name``, ``services``, and ``client_id`` as described below.
        app_name (str): Name of script/client. This will form the name of the token cache file.
                **Default**: ``'UNKNOWN'``.
        services (list of str): Services to authenticate with.
                **Default**: ``[]``.
        client_id (str): The ID of the client, given when registered with Globus.
                **Default**: The MDF Native Clients ID.
        make_clients (bool): If ``True``, will make and return appropriate clients with
                generated tokens. If ``False``, will only return authorizers.
                **Default**: ``True``.
        clear_old_tokens (bool): If ``True``, delete old token file if it exists,
                forcing user to re-login. If ``False``, use existing token file if there is one.
                **Default**: ``False``.
        token_dir (str): The path to the directory to save tokens in and look for
                credentials by default. **Default**: ``DEFAULT_CRED_PATH``.

    Returns:
        dict: The clients and authorizers requested, indexed by service name.
                For example, if ``login()`` is told to auth with ``'search'``
                then the search client will be in the ``'search'`` field.

        Note:
            Previously requested tokens (which are cached) will be returned alongside
            explicitly requested ones.
    """
    NATIVE_CLIENT_ID = "98bfc684-977f-4670-8669-71f8337688e4"
    DEFAULT_CRED_FILENAME = "globus_login.json"

    def _get_tokens(client, scopes, app_name, force_refresh=False):
        token_path = os.path.join(token_dir, app_name + "_tokens.json")
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
                os.makedirs(token_dir)
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
                    with open(os.path.join(token_dir, DEFAULT_CRED_FILENAME)) as cred_file:
                        creds = json.load(cred_file)
                except IOError:
                    raise ValueError("Credentials/configuration must be passed as a "
                                     + "filename string, JSON string, or dictionary, "
                                     + "or provided in '"
                                     + DEFAULT_CRED_FILENAME
                                     + "' or '"
                                     + token_dir
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
                                                authorizer=all_authorizers.pop(token_key),
                                                http_timeout=STD_TIMEOUT)
                except globus_sdk.GlobusAPIError as e:
                    print("Error: Unable to create {} client: {}".format(service, e.message))
            # If no applicable client, just translate the key
            else:
                returnables[service] = all_authorizers.pop(token_key)
    # Add authorizers not associated with service to returnables
    returnables.update(all_authorizers)

    return returnables


def confidential_login(credentials=None, client_id=None, client_secret=None, services=None,
                       make_clients=True, token_dir=DEFAULT_CRED_PATH):
    """Log in to Globus services as a confidential client
    (a client with its own login information).

    Arguments:
        credentials (str or dict): A string filename, string JSON, or dictionary
                with credential and config information.
                By default, uses the ``DEFAULT_CRED_FILENAME`` and token_dir.
                Contains ``client_id``, ``client_secret``, and ``services`` as defined below.
        client_id (str): The ID of the client.
        client_secret (str): The client's secret for authentication.
        services (list of str): Services to authenticate with.
        make_clients (bool): If ``True``, will make and return appropriate clients
                with generated tokens.
                If ``False``, will only return authorizers.
                **Default**: ``True``.
        token_dir (str): The path to the directory to save tokens in and look for
                credentials by default.
                **Default**: ``DEFAULT_CRED_PATH``.

    Returns:
        dict: The clients and authorizers requested, indexed by service name.
    """
    DEFAULT_CRED_FILENAME = "confidential_globus_login.json"
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
                    with open(os.path.join(token_dir, DEFAULT_CRED_FILENAME)) as cred_file:
                        creds = json.load(cred_file)
                except IOError:
                    raise ValueError("Credentials/configuration must be passed as a "
                                     "filename string, JSON string, or dictionary, or provided "
                                     "in '{}' or '{}'.".format(DEFAULT_CRED_FILENAME,
                                                               token_dir))
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
                                                authorizer=all_authorizers.pop(token_key),
                                                http_timeout=STD_TIMEOUT)
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


def logout(token_dir=DEFAULT_CRED_PATH):
    """Remove ALL tokens in the token directory.
    This will force re-authentication to all services.

    Arguments:
        token_dir (str): The path to the directory to save tokens in and look for
                credentials by default. If this argument was given to a ``login()`` function,
                the same value must be given here to properly logout.
                **Default**: ``DEFAULT_CRED_PATH``.
    """
    for f in os.listdir(token_dir):
        if f.endswith("tokens.json"):
            try:
                os.remove(os.path.join(token_dir, f))
            except OSError as e:
                # Eat ENOENT (no such file/dir, tokens already deleted) only,
                # raise any other issue (bad permissions, etc.)
                if e.errno != errno.ENOENT:
                    raise


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

def format_gmeta(data, acl=None, identifier=None):
    """Format input into GMeta format, suitable for ingesting into Globus Search.
    Formats a dictionary into a GMetaEntry.
    Formats a list of GMetaEntry into a GMetaList inside a GMetaIngest.

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
            "subject": identifier,
            "visible_to": prefixed_acl,
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
        for con in res["content"]:
            results.append(con)
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
        try:
            transfer_client.operation_ls(source_ep, path=item[0])
            source_is_dir = True
        except globus_sdk.exc.TransferAPIError as e:
            # If error indicates path exists but is not dir, is not dir
            if e.code == "ExternalError.DirListingFailed.NotDirectory":
                source_is_dir = False
            # Too many files in dir indicates is dir
            elif e.code == "ExternalError.DirListingFailed.SizeLimit":
                source_is_dir = True
            # Not found is real error
            elif e.code == "ClientError.NotFound":
                raise globus_sdk.GlobusError("Path '{}' not found on source endpoint '{}'"
                                             .format(item[0], source_ep))
            # Else, retry on parent dir
            else:
                try:
                    parent, item_name = os.path.split(item[0])
                    parent_ls = transfer_client.operation_ls(source_ep, path=parent)
                    type_list = [x["type"] for x in parent_ls["DATA"] if x["name"] == item_name]
                    if len(type_list) < 1:
                        raise globus_sdk.GlobusError("No items with name '{}' in path '{}' on "
                                                     "endpoint '{}'"
                                                     .format(item_name, parent, source_ep))
                    elif len(type_list) > 1:
                        raise globus_sdk.GlobusError("Multiple items with name '{}' in path '{}'"
                                                     "on endpoint '{}'"
                                                     .format(item_name, parent, source_ep))
                    item_type = type_list[0]
                    if item_type == "dir":
                        source_is_dir = True
                    elif item_type == "file":
                        source_is_dir = False
                    else:
                        raise ValueError("Path '{}' does not lead to a file or a directory ({})"
                                         .format(item[0], item_type))
                except globus_sdk.exc.TransferAPIError as e:
                    # Size limit means we can't figure out this path
                    if e.code == "ExternalError.DirListingFailed.SizeLimit":
                        raise globus_sdk.GlobusError("Unable to check type of {}".format(item[0]))
                    # Not found is still an error
                    elif e.code == "ClientError.NotFound":
                        raise globus_sdk.GlobusError("Parent path '{}' not found on source "
                                                     "endpoint '{}'".format(item[0], source_ep))
                    else:
                        raise

        # Check if dest path is directory
        dest_exists = False
        try:
            transfer_client.operation_ls(dest_ep, path=item[1])
            dest_exists = True
            dest_is_dir = True
        except globus_sdk.exc.TransferAPIError as e:
            if e.code == "ExternalError.DirListingFailed.NotDirectory":
                dest_exists = True
                dest_is_dir = False
            elif e.code == "ExternalError.DirListingFailed.SizeLimit":
                dest_exists = True
                dest_is_dir = True
            elif e.code == "ClientError.NotFound":
                # Destination will be created, not an issue if not found
                pass
            else:
                try:
                    parent, item_name = os.path.split(item[1])
                    parent_ls = transfer_client.operation_ls(dest_ep, path=parent)
                    type_list = [x["type"] for x in parent_ls["DATA"] if x["name"] == item_name]
                    if len(type_list) < 1:
                        raise globus_sdk.GlobusError("No items with name '{}' in path '{}' on "
                                                     "endpoint '{}'"
                                                     .format(item_name, parent, dest_ep))
                    elif len(type_list) > 1:
                        raise globus_sdk.GlobusError("Multiple items with name '{}' in path '{}'"
                                                     "on endpoint '{}'"
                                                     .format(item_name, parent, dest_ep))
                    item_type = type_list[0]
                    if item_type == "dir":
                        dest_exists = True
                        dest_is_dir = True
                    elif item_type == "file":
                        dest_exists = True
                        dest_is_dir = False
                    else:
                        # Assume we're overwriting whatever dest is, as if it doesn't exist
                        pass
                except globus_sdk.exc.TransferAPIError as e:
                    # Size limit means we can't figure out this path
                    if e.code == "ExternalError.DirListingFailed.SizeLimit":
                        raise globus_sdk.GlobusError("Unable to check type of {}".format(item[0]))
                    # Not found is not our problem for dest
                    elif e.code == "ClientError.NotFound":
                        pass
                    else:
                        raise
        # Transfer dir
        # Short-circuit OR/AND eval means if not dest_exists, dest_is_dir can be unassigned
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
# * Misc utilities
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


# *************************************************
# * Clients
# *************************************************

class _DataPublicationClient(BaseClient):

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
KNOWN_CLIENTS["publish"] = _DataPublicationClient
