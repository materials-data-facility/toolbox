from copy import deepcopy
import json

import globus_sdk


# *************************************************
# * Globus Search utilities
# *************************************************

SEARCH_INDEX_UUIDS = {
    "mdf": "1a57bbe5-5272-477f-9d31-343b8258b7a5",
    "mdf-test": "5acded0c-a534-45af-84be-dcf042e36412",
    "mdf-dev": "aeccc263-f083-45f5-ab1d-08ee702b3384",
    "dlhub": "847c9105-18a0-4ffb-8a71-03dd76dfcc9d",
    "dlhub-test": "5c89e0a9-00e5-4171-b415-814fe4d0b8af"
}


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
    if type(gmeta) is globus_sdk.GlobusHTTPResponse:
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
