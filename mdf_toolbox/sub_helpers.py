import warnings

from mdf_toolbox.search_helper import SearchHelper, SEARCH_LIMIT


class AggregateHelper(SearchHelper):
    """Subclass to add the ``aggregate()`` functionality to the SearchHelper.

    ``aggregate()`` is currently the only way to retrieve more than 10,000 entries
    from Globus Search, and requires a ``scroll_field`` index field.
    """

    def __init__(self, *args, **kwargs):
        """Add the AggregateHelper to a SearchHelper.

        Arguments:
            scroll_field (str): The field on which to scroll. This should be a field
                    that counts/indexes the entries.
        """
        self.scroll_field = kwargs.get("scroll_field", None)
        super().__init__(*args, **kwargs)

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
        # Make sure scroll_field is valid
        if not scroll_field:
            raise AttributeError("scroll_field is required.")

        # Make sure the query is set
        if not self.initialized:
            raise AttributeError('No query has been set.')

        # Warn the user if we are changing the setting of advanced
        if not self._SearchHelper__query["advanced"]:
            warnings.warn('This query will be run in advanced mode.', RuntimeWarning)
            self._SearchHelper__query["advanced"] = True

        # Inform the user if they set an invalid value for the query size
        if scroll_size <= 0:
            raise AttributeError('Scroll size must greater than zero')

        # Get the total number of records
        total = self.search(limit=0, info=True, reset_query=False)[1]["total_query_matches"]

        # If aggregate is unnecessary, use Search automatically instead
        if total <= SEARCH_LIMIT:
            return self.search(limit=SEARCH_LIMIT, reset_query=False)

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
                query = "({q}) AND ({field}:>={start} AND {field}:<{end})".format(
                        q=self._SearchHelper__query["q"], field=scroll_field, start=scroll_pos,
                        end=scroll_pos+scroll_width)

                results, info = self.search(q=query, advanced=True, info=True)

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

    def aggregate(self, q=None, scroll_size=SEARCH_LIMIT, reset_query=True, **kwargs):
        """Perform an advanced query, and return *all* matching results.
        Will automatically perform multiple queries in order to retrieve all results.

        Note:
            All ``aggregate`` queries run in advanced mode, and ``info`` is not available.

        Arguments:
            q (str): The query to execute. **Default:** The current helper-formed query, if any.
                    There must be some query to execute.
            scroll_size (int): Maximum number of records returned per query. Must be
                    between one and the ``SEARCH_LIMIT`` (inclusive).
                    **Default:** ``SEARCH_LIMIT``.
            reset_query (bool): If ``True``, will destroy the current query after execution
                    and start a fresh one.
                    If ``False``, will keep the current query set.
                    **Default:** ``True``.

        Keyword Arguments:
            scroll_field (str): The field on which to scroll. This should be a field
                    that counts/indexes the entries.
                    This should be set in ``self.scroll_field``, but if your application
                    requires separate scroll fields for a single client,
                    it can be set in this way as well.
                    **Default**: ``self.scroll_field``.

        Returns:
            list of dict: All matching records.
        """
        scroll_field = kwargs.get("scroll_field", self.scroll_field)

        # If q not specified, use internal, helper-built query
        if q is None:
            res = self._aggregate(scroll_field=scroll_field, scroll_size=scroll_size)
            if reset_query:
                self.reset_query()
            return res
        # Otherwise, run an independent query as SearchHelper.search() does.
        else:
            return self.__class__(index=self.index, q=q, advanced=True,
                                  search_client=self._SearchHelper__search_client
                                  ).aggregate(scroll_size=scroll_size, reset_query=reset_query)
