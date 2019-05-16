from copy import deepcopy
import warnings

import globus_sdk

import mdf_toolbox


# Maximum number of results per search allowed by Globus Search
SEARCH_LIMIT = 10000

# Maximum number of results to return when advanced=False
NONADVANCED_LIMIT = 10

# List of allowed operators
OP_LIST = ["AND", "OR", "NOT"]

# List of characters that should trigger automatic quotation marks
QUOTE_LIST = [" ", "\t", "\n", "'", ".", "?", ":", "^"]
# List of characters that should disable automatic quotation marks
# ex. range queries
UNQUOTE_LIST = ["[", "]", "{", "}"]

# Initial blank query
BLANK_QUERY = {
    "q": "(",
    "advanced": False,
    "limit": None,  # This is modified in _validate_query if not set
    "offset": 0,
    "facets": [],
    "filters": [],
    "sort": []
}


# ***********************************************
# * Static internal utility functions
# ***********************************************

def _clean_query_string(q):
    """Clean up a query string for searching.

    Removes unmatched parentheses and joining operators.

    Arguments:
        q (str): Query string to be cleaned

    Returns:
        str: The clean query string.
    """
    q = q.replace("()", "").strip()
    if q.endswith("("):
        q = q[:-1].strip()
    # Remove misplaced AND/OR/NOT at end
    if q[-3:] == "AND" or q[-3:] == "NOT":
        q = q[:-3]
    elif q[-2:] == "OR":
        q = q[:-2]

    # Balance parentheses
    while q.count("(") > q.count(")"):
        q += ")"
    while q.count(")") > q.count("("):
        q = "(" + q

    return q.strip()


def _validate_query(query):
    """Validate and clean up a query to be sent to Search.
    Cleans the query string, removes unneeded parameters, and validates for correctness.
    Does not modify the original argument.
    Raises an Exception on invalid input.

    Arguments:
        query (dict): The query to validate.

    Returns:
        dict: The validated query.
    """
    query = deepcopy(query)
    # q is always required
    if query["q"] == BLANK_QUERY["q"]:
        raise ValueError("No query specified.")

    query["q"] = _clean_query_string(query["q"])

    # limit should be set to appropriate default if not specified
    if query["limit"] is None:
        query["limit"] = SEARCH_LIMIT if query["advanced"] else NONADVANCED_LIMIT
    # If specified, the limit should not be greater than the Search maximum
    elif query["limit"] > SEARCH_LIMIT:
        warnings.warn('Reduced result limit from {} to the Search maximum: {}'
                      .format(query["limit"], SEARCH_LIMIT), RuntimeWarning)
        query["limit"] = SEARCH_LIMIT

    # Remove all blank/default values
    for key, val in BLANK_QUERY.items():
        # Default for get is NaN so comparison is always False
        if query.get(key, float('nan')) == val:
            query.pop(key)

    # Remove unsupported fields
    to_remove = [field for field in query.keys() if field not in BLANK_QUERY.keys()]
    [query.pop(field) for field in to_remove]

    return query


# ****************************************************************************************
# * SearchHelper
# ****************************************************************************************

class SearchHelper:
    """Utility class for performing queries using a ``globus_sdk.SearchClient``.

    Notes:
        Query strings may end up wrapped in parentheses, which has no direct effect on the search.
        It is inadvisable to use the "private" methods to modify the query string directly,
        as the low-level logic for query string generation is not as user-friendly.
    """
    __app_name = "SearchHelper_Client"

    def __init__(self, index, **kwargs):
        """Create a SearchHelper object.

        Arguments:
            index (str): The Globus Search index to search on.

        Keyword Arguments:
            search_client (globus_sdk.SearchClient): The Globus Search client to use for
                    searching. If not provided, one will be created and the user may be asked
                    to log in. **Default**: ``None``.
            anonymous (bool): If ``True``, will not authenticate with Globus Auth.
                    If ``False``, will require authentication (either a SearchClient or
                    a user-interactive login).
                    **Default:** ``False``.

                    Caution:
                        Authentication is required to view non-public data in Search.
                        An anonymous SearchHelper will only return public results.

            app_name (str): The application name to use. Should be changed for
                    subclassed clients, and left alone otherwise.
                    Only used if performing login flow.
                    **Default**: ``"SearchHelper_Client"``.
            client_id (str): The ID of a native client to use when authenticating.
                    Only used if performing login flow.
                    **Default**: The ID of the MDF Native Clients native client.

            q (str): A query string to initialize the SearchHelper with.
                    Intended for internal use.
            advanced (bool): The initial advanced state for thie SearchHelper.
                    Intended for internal use.
        """
        if kwargs.get("search_client"):
            self.__search_client = kwargs["search_client"]
        elif kwargs.get("anonymous"):
            self.__search_client = mdf_toolbox.anonymous_login(["search"])["search"]
        else:
            self.__search_client = mdf_toolbox.login(
                                        app_name=kwargs.get("app_name", self.__app_name),
                                        client_id=kwargs.get("client_id", None),
                                        services=["search"])["search"]

        # Get the UUID for the index if the name was provided
        self.index = mdf_toolbox.translate_index(index)

        # Query init
        self.__query = deepcopy(BLANK_QUERY)
        if kwargs.get("q"):
            self.__query["q"] = kwargs["q"]
        if kwargs.get("advanced"):
            self.__query["advanced"] = kwargs["advanced"]

    @property
    def initialized(self):
        """Whether any valid term has been added to the query."""
        return bool(self._clean_query())

    def logout(self):
        """Delete Globus Auth tokens."""
        mdf_toolbox.logout()

    # ************************************************************************************
    # * Internal functions
    # ************************************************************************************

    def _clean_query(self):
        """Returns the current query, cleaned for user consumption.

        Returns:
            str: The clean current query.
        """
        return _clean_query_string(self.__query["q"])

    def _term(self, term):
        """Add a term to the query.

        Arguments:
            term (str): The term to add.

        Returns:
            SearchHelper: Self
        """
        # All terms must be strings for Elasticsearch
        term = str(term)
        if term:
            self.__query["q"] += term
        return self

    def _field(self, field, value):
        """Add a ``field:value`` term to the query.
        Matches will have the ``value`` in the ``field``.

        Note:
            This method triggers advanced mode.

        Arguments:
            field (str): The field to check for the value, in Elasticsearch dot syntax.
            value (str): The value to match.

        Returns:
            SearchHelper: Self
        """
        # Fields and values must be strings for Elasticsearch
        field = str(field)
        value = str(value)

        # Check if quotes required and allowed, and quotes not present
        # If the user adds improper double-quotes, this will not fix them
        if (any([char in value for char in QUOTE_LIST]) and '"' not in value
                and not any([char in value for char in UNQUOTE_LIST])):
            value = '"' + value + '"'

        # Cannot add field:value if one is blank
        if field and value:
            self.__query["q"] += field + ":" + value
            # Field matches are advanced queries
            self.__query["advanced"] = True

        return self

    def _operator(self, op, close_group=False):
        """Add an operator between terms.
        There must be a term added before using this method.
        All operators have helpers, so this method is usually not necessary to directly invoke.

        Arguments:
            op (str): The operator to add. Must be in the OP_LIST.
            close_group (bool): If ``True``, will end the current parenthetical
                group and start a new one.
                If ``False``, will continue current group.

                Example::
                    "(foo AND bar)" is one group.
                    "(foo) AND (bar)" is two groups.

        Returns:
            SearchHelper: Self
        """
        op = op.upper().strip()
        if op not in OP_LIST:
            raise ValueError("Error: '{}' is not a valid operator.".format(op))
        else:
            if close_group:
                op = ") " + op + " ("
            else:
                op = " " + op + " "
            self.__query["q"] += op
        return self

    def _and_join(self, close_group=False):
        """Combine terms with AND.
        There must be a term added before using this method.

        Arguments:
            close_group (bool): If ``True``, will end the current group and start a new one.
                    If ``False``, will continue current group.

                    Example::

                        If the current query is "(term1"
                        .and(close_group=True) => "(term1) AND ("
                        .and(close_group=False) => "(term1 AND "

        Returns:
            SearchHelper: Self
        """
        if not self.initialized:
            raise ValueError("You must add a search term before adding an operator.")
        else:
            self._operator("AND", close_group=close_group)
        return self

    def _or_join(self, close_group=False):
        """Combine terms with OR.
        There must be a term added before using this method.

        Arguments:
            close_group (bool): If ``True``, will end the current group and start a new one.
                    If ``False``, will continue current group.

                    Example:

                        If the current query is "(term1"
                        .or(close_group=True) => "(term1) OR("
                        .or(close_group=False) => "(term1 OR "

        Returns:
            SearchHelper: Self
        """
        if not self.initialized:
            raise ValueError("You must add a search term before adding an operator.")
        else:
            self._operator("OR", close_group=close_group)
        return self

    def _negate(self):
        """Negates the next added term with NOT.

        Returns:
            SearchHelper: Self
        """
        self._operator("NOT")
        return self

    def _add_sort(self, field, ascending=True):
        """Sort the search results by a certain field.

        If this method is called multiple times, the later sort fields are given lower priority,
        and will only be considered when the eariler fields have the same value.

        Arguments:
            field (str): The field to sort by, in Elasticsearch dot syntax.
            ascending (bool): Sort in ascending order? **Default**: ``True``.

        Returns:
            SearchHelper: Self
        """
        # Fields must be strings for Elasticsearch
        field = str(field)
        # No-op on blank sort field
        if field:
            self.__query["sort"].append({
                'field_name': field,
                'order': 'asc' if ascending else 'desc'
            })
        return self

    def _ex_search(self, limit=None, info=False, retries=3):
        """Execute a search and return the results, up to the ``SEARCH_LIMIT``.

        Uses the query currently in this SearchHelper.

        Arguments:
            limit (int): Maximum number of entries to return. **Default**: ``10`` for basic
                queries, and ``10000`` for advanced.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.
            retries (int): The number of times to retry a Search query if it fails.
                           **Default:** 3.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.
        """
        # Make sure there is query information present
        if not self.initialized:
            raise ValueError('No query has been set.')

        # Create Search-ready query
        if limit is not None:
            self.__query["limit"] = limit
        query = _validate_query(self.__query)

        tries = 0
        errors = []
        while True:
            # Try searching until success or `retries` number of failures
            # Raise exception after `retries` failures
            try:
                search_res = self.__search_client.post_search(self.index, query)
            except globus_sdk.SearchAPIError as e:
                if tries >= retries:
                    raise
                else:
                    errors.append(repr(e))
            except Exception as e:
                if tries >= retries:
                    raise
                else:
                    errors.append(repr(e))
            else:
                break
            tries += 1

        # Remove the wrapping on each entry from Globus search
        res = mdf_toolbox.gmeta_pop(search_res, info=info)

        # Add more information to output if requested
        if info:
            # Add everything from the query itself
            info_dict = mdf_toolbox.dict_merge(res[1], query)
            # But rename "q" to "query" for clarity
            info_dict["query"] = info_dict.pop("q")
            # Add other useful/interesting parameters
            info_dict["index_uuid"] = self.index
            info_dict["retries"] = tries
            info_dict["errors"] = errors
            # Remake tuple because tuples don't suport assignment
            res = (res[0], info_dict)
        return res

    def _mapping(self):
        """Fetch the entire mapping for the specified index.

        Returns:
            dict: The full mapping for the index.
        """
        return (self.__search_client.get(
                    "/unstable/index/{}/mapping".format(mdf_toolbox.translate_index(self.index)))
                ["mappings"])

    # ************************************************************************************
    # * Query-building functions
    # ************************************************************************************
    # Note: Only match_term, match_field, exclude_field, and add_sort directly modify
    #       the query. The other helpers use those core functions for advanced behavior.
    # ************************************************************************************

    def match_term(self, value, required=True, new_group=False):
        """Add a fulltext search term to the query.

        Warning:
            Do not use this method with any other query-building helpers. This method
            is only for building fulltext queries (in non-advanced mode). Using other
            helpers, such as ``match_field()``, will cause the query to run in advanced mode.
            If a fulltext term query is run in advanced mode, it will have unexpected
            results.

        Arguments:
            value (str): The term to match.
            required (bool): If ``True``, will add term with ``AND``.
                    If ``False``, will use ``OR``. **Default:** ``True``.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        # If not the start of the query string, add an AND or OR
        if self.initialized:
            if required:
                self._and_join(new_group)
            else:
                self._or_join(new_group)
        self._term(value)
        return self

    def match_field(self, field, value, required=True, new_group=False):
        """Add a ``field:value`` term to the query.
        Matches will have the ``value`` in the ``field``.

        Arguments:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            value (str): The value to match.
            required (bool): If ``True``, will add term with ``AND``.
                    If ``False``, will use ``OR``. **Default:** ``True``.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        # If not the start of the query string, add an AND or OR
        if self.initialized:
            if required:
                self._and_join(new_group)
            else:
                self._or_join(new_group)
        self._field(field, value)
        return self

    def exclude_field(self, field, value, new_group=False):
        """Exclude a ``field:value`` term from the query.
        Matches will NOT have the ``value`` in the ``field``.

        Arguments:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            value (str): The value to exclude.
            new_group (bool): If ``True``, will separate term the into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        # No-op on missing arguments
        if not field and not value:
            return self
        # If not the start of the query string, add an AND
        # OR would not make much sense for excluding
        if self.initialized:
            self._and_join(new_group)
        self._negate()._field(str(field), str(value))
        return self

    def match_exists(self, field, required=True, new_group=False):
        """Require a field to exist in the results.
        Matches will have some value in ``field``.

        Arguments:
            field (str): The field to check.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            required (bool): If ``True``, will add term with ``AND``.
                    If ``False``, will use ``OR``. **Default:** ``True``.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        return self.match_field(field, "*", required=required, new_group=new_group)

    def match_not_exists(self, field, new_group=False):
        """Require a field to not exist in the results.
        Matches will not have ``field`` present.

        Arguments:
            field (str): The field to check.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        return self.exclude_field(field, "*", new_group=new_group)

    def match_range(self, field, start=None, stop=None, inclusive=True,
                    required=True, new_group=False):
        """Add a ``field:[some range]`` term to the query.
        Matches will have a ``value`` in the range in the ``field``.

        Arguments:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            start (str or int): The starting value, or ``None`` for no lower bound.
                    **Default:** ``None``.
            stop (str or int): The ending value, or ``None`` for no upper bound.
                    **Default:** ``None``.
            inclusive (bool): If ``True``, the ``start`` and ``stop`` values will be included
                    in the search.
                    If ``False``, the start and stop values will not be included
                    in the search.
                    **Default:** ``True``.
            required (bool): If ``True``, will add term with ``AND``.
                    If ``False``, will use ``OR``. **Default:** ``True``.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        # Accept None as *
        if start is None:
            start = "*"
        if stop is None:
            stop = "*"
        # *-* is the same as field exists
        if start == "*" and stop == "*":
            return self.match_exists(field, required=required, new_group=new_group)

        if inclusive:
            value = "[" + str(start) + " TO " + str(stop) + "]"
        else:
            value = "{" + str(start) + " TO " + str(stop) + "}"
        return self.match_field(field, value, required=required, new_group=new_group)

    def exclude_range(self, field, start="*", stop="*", inclusive=True, new_group=False):
        """Exclude a ``field:[some range]`` term from the query.
        Matches will not have any ``value`` in the range in the ``field``.

        Arguments:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            start (str or int): The starting value, or ``None`` for no lower bound.
                    **Default:** ``None``.
            stop (str or int): The ending value, or ``None`` for no upper bound.
                    **Default:** ``None``.
            inclusive (bool): If ``True``, the ``start`` and ``stop`` values will be excluded
                    from the search.
                    If ``False``, the ``start`` and ``stop`` values will not be excluded
                    from the search.
                    **Default:** ``True``.
            new_group (bool): If ``True``, will separate the term into a new parenthetical group.
                    If ``False``, will not.
                    **Default:** ``False``.

        Returns:
            SearchHelper: Self
        """
        # Accept None as *
        if start is None:
            start = "*"
        if stop is None:
            stop = "*"
        # *-* is the same as field doesn't exist
        if start == "*" and stop == "*":
            return self.match_not_exists(field, new_group=new_group)

        if inclusive:
            value = "[" + str(start) + " TO " + str(stop) + "]"
        else:
            value = "{" + str(start) + " TO " + str(stop) + "}"
        return self.exclude_field(field, value, new_group=new_group)

    def exclusive_match(self, field, value):
        """Match exactly the given value(s), with no other data in the field.

        Arguments:
            field (str): The field to check for the value.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            value (str or list of str): The value(s) to match exactly.

        Returns:
            SearchHelper: Self
        """
        if isinstance(value, str):
            value = [value]

        # Hacky way to get ES to do exclusive search
        # Essentially have a big range search that matches NOT anything
        # Except for the actual values
        # Example: [foo, bar, baz] =>
        #   (NOT {* TO foo} AND [foo TO foo] AND NOT {foo to bar} AND [bar TO bar]
        #    AND NOT {bar TO baz} AND [baz TO baz] AND NOT {baz TO *})
        # Except it must be sorted to not overlap
        value.sort()

        # Start with removing everything before first value
        self.exclude_range(field, "*", value[0], inclusive=False, new_group=True)
        # Select first value
        self.match_range(field, value[0], value[0])
        # Do the rest of the values
        for index, val in enumerate(value[1:]):
            self.exclude_range(field, value[index-1], val, inclusive=False)
            self.match_range(field, val, val)
        # Add end
        self.exclude_range(field, value[-1], "*", inclusive=False)
        # Done
        return self

    def add_sort(self, field, ascending=True):
        """Sort the search results by a certain field.

        If this method is called multiple times, the later sort fields are given lower priority,
        and will only be considered when the eariler fields have the same value.

        Arguments:
            field (str): The field to sort by.
                    The field must be namespaced according to Elasticsearch rules
                    using the dot syntax.
                    For example, ``"mdf.source_name"`` is the ``source_name`` field
                    of the ``mdf`` dictionary.
            ascending (bool): If ``True``, the results will be sorted in ascending order.
                    If ``False``, the results will be sorted in descending order.
                    **Default**: ``True``.
        Returns:
            SearchHelper: Self
        """
        # No-op on blank field
        if not field:
            return self
        self._add_sort(field, ascending=ascending)
        return self

    # ************************************************************************************
    # * Execution functions
    # ************************************************************************************

    def search(self, q=None, advanced=False, limit=None, info=False, reset_query=True):
        """Execute a search and return the results, up to the ``SEARCH_LIMIT``.

        Arguments:
            q (str): The query to execute. **Default:** The current helper-formed query, if any.
                    There must be some query to execute.
            advanced (bool): Whether to treat ``q`` as a basic or advanced query.
                Has no effect if a query is not supplied in ``q``.
                **Default:** ``False``
            limit (int): The maximum number of results to return.
                    The max for this argument is the ``SEARCH_LIMIT`` imposed by Globus Search.
                    **Default:** ``SEARCH_LIMIT`` for advanced queries, 10 for basic queries.
            info (bool): If ``False``, search will return a list of the results.
                    If ``True``, search will return a tuple containing the results list
                    and other information about the query.
                    **Default:** ``False``.
            reset_query (bool): If ``True``, will destroy the current query after execution
                    and start a fresh one.
                    If ``False``, will keep the current query set.
                    Has no effect if a query is supplied in ``q``.
                    **Default:** ``True``.

        Returns:
            If ``info`` is ``False``, *list*: The search results.
            If ``info`` is ``True``, *tuple*: The search results,
            and a dictionary of query information.

        Note:
            If a query is specified in ``q``, the current, helper-built query (if any)
            will not be used in the search or modified.
        """
        # If q not specified, use internal, helper-built query
        if q is None:
            res = self._ex_search(info=info, limit=limit)
            if reset_query:
                self.reset_query()
            return res
        # If q was specified, run a totally independent query with a new SearchHelper
        # Init SearchHelper with query, then call .search(), which will use it
        # ._ex_search() not canonical way to perform single-statement search, so not used
        # reset_query is False to skip the unnecessary query reset - SH not needed after search
        else:
            return SearchHelper(index=self.index, search_client=self.__search_client, q=q,
                                advanced=advanced).search(info=info, limit=limit,
                                                          reset_query=False)

    # ************************************************************************************
    # * Query utility functions
    # ************************************************************************************

    def show_fields(self, block=None):
        """Retrieve and return the mapping for the given metadata block.

        Arguments:
            block (str): The top-level field to fetch the mapping for (for example, ``"mdf"``),
                    or the special values ``None`` for everything or ``"top"`` for just the
                    top-level fields.
                    **Default:** ``None``.
            index (str): The Search index to map. **Default:** The current index.

        Returns:
            dict: ``field:datatype`` pairs.
        """
        mapping = self._mapping()
        if block is None:
            return mapping
        elif block == "top":
            blocks = set()
            for key in mapping.keys():
                blocks.add(key.split(".")[0])
            block_map = {}
            for b in blocks:
                block_map[b] = "object"
        else:
            block_map = {}
            for key, value in mapping.items():
                if key.startswith(block):
                    block_map[key] = value
        return block_map

    def current_query(self):
        """Return the current query string.

        Returns:
            str: The current query.
        """
        return self._clean_query()

    def reset_query(self):
        """Destroy the current query and create a fresh one.
        This method should not be chained.

        Returns:
            None
        """
        self.__query = deepcopy(BLANK_QUERY)
        return
