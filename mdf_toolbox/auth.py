from fair_research_login import NativeClient
from globus_nexus_client import NexusClient
import globus_sdk


# *************************************************
# * Authentication utilities
# *************************************************

KNOWN_SCOPES = {
    "transfer": "urn:globus:auth:scope:transfer.api.globus.org:all",
    "search": "urn:globus:auth:scope:search.api.globus.org:search",
    "search_ingest": "urn:globus:auth:scope:search.api.globus.org:all",
    "data_mdf": "urn:globus:auth:scope:data.materialsdatafacility.org:all",
    "mdf_connect": "https://auth.globus.org/scopes/0e0a9538-ce45-43c1-998a-d3a7031a83f0/connect",
    "petrel": "https://auth.globus.org/scopes/56ceac29-e98a-440a-a594-b41e7a084b62/all",
    "groups": "urn:globus:auth:scope:nexus.api.globus.org:groups",
    "dlhub": "https://auth.globus.org/scopes/81fc4156-a623-47f2-93ad-7184118226ba/auth",
    "funcx": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
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
DEFAULT_APP_NAME = "UNNAMED_APP"
DEFAULT_CLIENT_ID = "984464e2-90ab-433d-8145-ac0215d26c8e"
STD_TIMEOUT = 5 * 60  # 5 minutes


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
