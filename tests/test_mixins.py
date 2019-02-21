import pytest
import re

import mdf_toolbox
from mdf_toolbox.search_helper import SearchHelper
from mdf_toolbox.mixins import AggregateMixin


SEARCH_CLIENT = mdf_toolbox.login(credentials={"app_name": "MDF_Forge",
                                               "services": ["search"]})["search"]
INDEX = "mdf"
SCROLL_FIELD = "mdf.scroll_id"


class DummyClient(AggregateMixin, SearchHelper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, scroll_field=SCROLL_FIELD, **kwargs)


# Helper
# Return codes:
#  -1: No match, the value was never found
#   0: Exclusive match, no values other than argument found
#   1: Inclusive match, some values other than argument found
#   2: Partial match, value is found in some but not all results
def check_field(res, field, regex):
    dict_path = ""
    for key in field.split("."):
        if key == "[]":
            dict_path += "[0]"
        else:
            dict_path += ".get('{}', {})".format(key, "{}")
    # If no results, set matches to false
    all_match = (len(res) > 0)
    only_match = (len(res) > 0)
    some_match = False
    for r in res:
        vals = eval("r"+dict_path)
        if vals == {}:
            vals = []
        elif type(vals) is not list:
            vals = [vals]
        # If a result does not contain the value, no match
        if regex not in vals and not any([re.search(str(regex), value) for value in vals]):
            all_match = False
            only_match = False
        # If a result contains other values, inclusive match
        elif len(vals) != 1:
            only_match = False
            some_match = True
        else:
            some_match = True

    if only_match:
        # Exclusive match
        return 0
    elif all_match:
        # Inclusive match
        return 1
    elif some_match:
        # Partial match
        return 2
    else:
        # No match
        return -1


def test_aggregate_internal(capsys):
    q = DummyClient(index=INDEX, search_client=SEARCH_CLIENT, advanced=True)
    # Error on no query
    with pytest.raises(AttributeError):
        q.aggregate()

    # Basic aggregation
    res1 = q.aggregate("mdf.source_name:nist_xps_db")
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)

    # Multi-dataset aggregation
    q._SearchHelper__query["q"] = "(mdf.source_name:nist_xps_db OR mdf.source_name:khazana_vasp)"
    res2 = q.aggregate()
    assert len(res2) > 10000
    assert len(res2) > len(res1)

    # Unnecessary aggregation fallback to .search()
    # Check success in Coveralls
    q._SearchHelper__query["q"] = "mdf.source_name:khazana_vasp"
    assert len(q.aggregate()) < 10000


def test_aggregate_external():
    # Test that aggregate uses the current query properly
    # And returns results
    # And respects the reset_query arg
    f = DummyClient(INDEX, search_client=SEARCH_CLIENT)
    f.match_field("mdf.source_name", "nist_xps_db")
    res1 = f.aggregate(reset_query=False, index="mdf")
    assert len(res1) > 10000
    assert check_field(res1, "mdf.source_name", "nist_xps_db") == 0
    res2 = f.aggregate()
    assert len(res2) == len(res1)
    assert check_field(res2, "mdf.source_name", "nist_xps_db") == 0
