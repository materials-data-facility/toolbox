from mdf_toolbox.search_helper import SEARCH_LIMIT, _validate_query


class AggregateMixin:
    """Mixin to add the ``aggregate()`` functionality to the SearchHelper.

    ``aggregate()`` is currently the only way to retrieve more than 10,000 entries
    from Globus Search, and requires a ``scroll_field`` index field.
    """

    def _aggregate(self, scroll_field, scroll_size=SEARCH_LIMIT):
        """Perform an advanced query, and return *all* matching results.
        Will automatically perform multiple queries in order to retrieve all results.

        Note: All ``aggregate`` queries run in advanced mode.

        Arguments:
            scroll_field (str): The field on which to scroll. This should be a field
                    that counts/indexes the entries.
            scroll_size (int): Maximum number of records returned per query. Must be
                    between one and the ``SEARCH_LIMIT`` (inclusive).
                    **Default:** ``SEARCH_LIMIT``.

        Returns:
            list of dict: All matching entries.
        """
        # Warn the user we are changing the setting of advanced
        if not self.__query["advanced"]:
            warnings.warn('This query will be run in advanced mode.', RuntimeWarning)

        # Make sure the query has been set
        q = self.clean_query()
        if not q.strip("()"):
            raise ValueError('Query not set')

        # Inform the user if they set an invalid value for the query size
        if scroll_size <= 0:
            raise AttributeError('Scroll size must greater than zero')

        # Get the total number of records
        total = Query(self.__search_client, q,
                      advanced=True).search(index, limit=0, info=True)[1]["total_query_matches"]

        # If aggregate is unnecessary, use Search automatically instead
        if total <= SEARCH_LIMIT:
            return Query(self.__search_client, q, advanced=True).search(index)

        # Scroll until all results are found
        output = []

        scroll_pos = 0
        while len(output) < total:

            # Scroll until the width is small enough to get all records
            #   `scroll_id`s are unique to each dataset. If multiple datasets
            #   match a certain query, the total number of matching records
            #   may exceed the maximum that search will return - even if the
            #   scroll width is much smaller than that maximum
            scroll_width = scroll_size
            while True:
                query = "(" + q + ') AND (mdf.scroll_id:>=%d AND mdf.scroll_id:<%d)' % (
                                        scroll_pos, scroll_pos+scroll_width)
                results, info = Query(self.__search_client, query,
                                      advanced=True).search(index, info=True, limit=scroll_size)

                # Check to make sure that all the matching records were returned
                if info["total_query_matches"] <= len(results):
                    break

                # If not, reduce the scroll width
                # new_width is proportional with the proportion of results returned
                new_width = scroll_width * (len(results) // info["total_query_matches"])

                # scroll_width should never be 0, and should only be 1 in rare circumstances
                scroll_width = new_width if new_width > 1 else max(scroll_width//2, 1)

            # Append the results to the output
            output.extend(results)
            scroll_pos += scroll_width

        return output

    def aggregate(self, q=None, index=None, scroll_size=SEARCH_LIMIT, reset_query=True):
        """Perform an advanced query, and return *all* matching results.
        Will automatically perform multiple queries in order to retrieve all results.

        Note:
            All ``aggregate`` queries run in advanced mode, and ``info`` is not available.

        Arguments:
            q (str): The query to execute. **Default:** The current helper-formed query, if any.
                    There must be some query to execute.
            index (str): The Search index to search on. **Default:** The current index.
            scroll_size (int): Maximum number of records returned per query. Must be
                    between one and the ``SEARCH_LIMIT`` (inclusive).
                    **Default:** ``SEARCH_LIMIT``.
            reset_query (bool): If ``True``, will destroy the current query after execution
                    and start a fresh one.
                    If ``False``, will keep the current query set.
                    **Default:** ``True``.

        Returns:
            list of dict: All matching records.
        """

        # Get the desired index
        if not index:
            index = self.index

        if q is None:
            res = self.__query.aggregate(index=index, scroll_size=scroll_size)
            if reset_query:
                self.reset_query()
            return res
        else:
            return Query(self.__search_client, q=q,
                         advanced=True).aggregate(self.index, scroll_size=scroll_size)
