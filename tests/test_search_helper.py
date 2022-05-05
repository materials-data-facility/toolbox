from copy import deepcopy
import re

from globus_sdk import SearchAPIError
import pytest

import mdf_toolbox
from mdf_toolbox.globus_search.search_helper import (SearchHelper, _validate_query,
                                                     BLANK_QUERY, SEARCH_LIMIT)

#github specific declarations
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

search_auth = mdf_toolbox.confidential_login(client_id=client_id,
                                        client_secret=client_secret,
                                        services=['search'], make_clients=True)

# Manually logging in for SearchHelper testing
SEARCH_CLIENT = mdf_toolbox.login(services=["search"], app_name="SearchHelper",
                                  client_id="878721f5-6b92-411e-beac-830672c0f69a")["search"]
INDEX = "mdf"

# For purely historical reasons, internal-function tests create a SearchHelper
# called "q" while external-function tests use "f". This was a meaningful distinction
# when internal functions were in Query and external in Forge, but is not now.


# ***********************************************
# * Static functions
# ***********************************************

# _clean_query_string() effectively tested in test_clean_query()

def test_validate_query():
    # Error on no query
    with pytest.raises(ValueError):
        _validate_query(deepcopy(BLANK_QUERY))

    # If all fields set correctly, no changes
    query1 = deepcopy(BLANK_QUERY)
    query1["q"] = "(mdf.source_name:oqmd)"
    query1["advanced"] = True
    query1["limit"] = 1234
    query1["offset"] = 5
    query1["facets"] = "GFacet document"
    query1["filters"] = "GFilter document"
    query1["sort"] = "GSort document"
    res1 = _validate_query(query1)
    assert query1 == res1
    assert query1 is not res1

    query2 = deepcopy(BLANK_QUERY)
    # q and limit get corrected
    query2["q"] = "(mdf.source_name:oqmd("
    query2["limit"] = 20000
    # None for offset is invalid normally, but should not be removed
    # because it is not the default value - the user has set it
    query2["offset"] = None
    # No errors on missing data
    query2.pop("filters")
    # Unsupported fields get removed
    query2["badfield"] = "yes"
    res2 = _validate_query(query2)
    # Default values get removed
    assert res2 == {
        "q": "(mdf.source_name:oqmd)",
        "limit": SEARCH_LIMIT,
        "offset": None
    }


# ***********************************************
# * Internals
# ***********************************************

def test_init():
    q1 = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    assert q1._SearchHelper__query["q"] == "("
    assert q1._SearchHelper__query["advanced"] is False
    assert q1.initialized is False

    q2 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="mdf.source_name:oqmd", advanced=True)
    assert q2._SearchHelper__query["q"] == "mdf.source_name:oqmd"
    assert q2._SearchHelper__query["advanced"] is True
    assert q2.initialized is True

    # Test without explicit SearchClient
    q3 = SearchHelper(INDEX)
    assert q3._SearchHelper__query["advanced"] is False
    assert q3.initialized is False


def test_term():
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Single match test
    assert isinstance(q._term("term1"), SearchHelper)
    assert q._SearchHelper__query["q"] == "(term1"
    assert q.initialized is True
    # Multi-match test
    q._and_join()._term("term2")
    assert q._SearchHelper__query["q"] == "(term1 AND term2"
    # Grouping test
    q._or_join(close_group=True)._term("term3")
    assert q._SearchHelper__query["q"] == "(term1 AND term2) OR (term3"


def test_field():
    q1 = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Single field and return value test
    assert isinstance(q1._field("mdf.source_name", "oqmd"), SearchHelper)
    assert q1._SearchHelper__query["q"] == "(mdf.source_name:oqmd"
    # Multi-field and grouping test
    q1._and_join(close_group=True)._field("dc.title", "sample")
    assert q1._SearchHelper__query["q"] == "(mdf.source_name:oqmd) AND (dc.title:sample"
    # Negation test
    q1._negate()
    assert q1._SearchHelper__query["q"] == "(mdf.source_name:oqmd) AND (dc.title:sample NOT "
    # Explicit operator test
    # Makes invalid query for this case
    q1._operator("NOT")
    assert q1._SearchHelper__query["q"] == "(mdf.source_name:oqmd) AND (dc.title:sample NOT  NOT "
    # Ensure advanced is set
    assert q1._SearchHelper__query["advanced"] is True

    # Test noop on blanks
    q2 = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    assert q2._SearchHelper__query["q"] == "("
    q2._field(field="", value="value")
    assert q2._SearchHelper__query["q"] == "("
    q2._field(field="field", value="")
    assert q2._SearchHelper__query["q"] == "("
    q2._field(field="", value="")
    assert q2._SearchHelper__query["q"] == "("
    q2._field(field="field", value="value")
    assert q2._SearchHelper__query["q"] == "(field:value"

    # Test auto-quote
    q3 = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    q3._field("dc.descriptions.description", "With Spaces")
    assert q3._SearchHelper__query["q"] == '(dc.descriptions.description:"With Spaces"'
    q3._and_join(close_group=True)._field("dc.title", "Mark's")
    assert q3._SearchHelper__query["q"] == ('(dc.descriptions.description:"With Spaces") AND ('
                                            'dc.title:"Mark\'s"')
    q3._or_join(close_group=False)._field("dc.title", "The\nLarch")
    assert q3._SearchHelper__query["q"] == ('(dc.descriptions.description:"With Spaces") AND ('
                                            'dc.title:"Mark\'s" OR dc.title:"The\nLarch"')
    # No auto-quote on ranges
    q3._and_join(close_group=True)._field("block.range", "[5 TO 6]")
    assert q3._SearchHelper__query["q"] == ('(dc.descriptions.description:"With Spaces") AND ('
                                            'dc.title:"Mark\'s" OR dc.title:"The\nLarch") AND ('
                                            'block.range:[5 TO 6]')


def test_operator():
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    assert q._SearchHelper__query["q"] == "("
    # Add bad operator
    with pytest.raises(ValueError):
        assert q._operator("FOO") == q
    assert q._SearchHelper__query["q"] == "("
    # Test operator cleaning
    q._operator("   and ")
    assert q._SearchHelper__query["q"] == "( AND "
    # Test close_group
    q._operator("OR", close_group=True)
    assert q._SearchHelper__query["q"] == "( AND ) OR ("


def test_and_join(capsys):
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Test not initialized
    with pytest.raises(ValueError) as excinfo:
        q._and_join()
    assert 'before adding an operator' in str(excinfo.value)

    # Regular join
    q._term("foo")._and_join()
    assert q._SearchHelper__query["q"] == "(foo AND "
    # close_group
    q._term("bar")._and_join(close_group=True)
    assert q._SearchHelper__query["q"] == "(foo AND bar) AND ("


def test_or_join(capsys):
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Test not initialized
    with pytest.raises(ValueError) as excinfo:
        q._or_join()
    assert 'before adding an operator' in str(excinfo.value)

    # Regular join
    q._term("foo")._or_join()
    assert q._SearchHelper__query["q"] == "(foo OR "

    # close_group
    q._term("bar")._or_join(close_group=True)
    assert q._SearchHelper__query["q"] == "(foo OR bar) OR ("


def test_ex_search():
    # Error on no query
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    with pytest.raises(ValueError):
        q._ex_search()

    # Return info if requested
    res2 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="Al")._ex_search(info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)
    res3 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="Al")._ex_search(info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    res4 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="Al")._ex_search(info=False,
                                                                               limit=3)
    assert len(res4) == 3

    # Check default limits
    res5 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="Al")._ex_search()
    assert len(res5) == 10
    res6 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="mdf.source_name:nist_xps_db",
                        advanced=True)._ex_search()
    assert len(res6) == 10000

    # Check limit correction (should throw a warning)
    with pytest.warns(RuntimeWarning):
        res7 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, advanced=True,
                            q="mdf.source_name:nist_xps_db")._ex_search(limit=20000)
    assert len(res7) == 10000

    # Test index translation
    # mdf = 1a57bbe5-5272-477f-9d31-343b8258b7a5
    res8 = SearchHelper(INDEX, search_client=SEARCH_CLIENT,
                        q="data")._ex_search(info=True, limit=1)
    assert len(res8[0]) == 1
    assert res8[1]["index_uuid"] == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    with pytest.raises(SearchAPIError):
        SearchHelper("notexists", search_client=SEARCH_CLIENT,
                     q="data")._ex_search(info=True, limit=1)


def test_chaining():
    # Internal
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    q._field("source_name", "cip")
    q._and_join()
    q._field("elements", "Al")
    res1 = q._ex_search(limit=10000)
    res2 = (SearchHelper(INDEX, search_client=SEARCH_CLIENT)
            ._field("source_name", "cip")
            ._and_join()
            ._field("elements", "Al")
            ._ex_search(limit=10000))
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])

    # External
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    f.match_field("source_name", "cip")
    f.match_field("material.elements", "Al")
    res1 = f.search()
    res2 = f.match_field("source_name", "cip").match_field("material.elements", "Al").search()
    assert all([r in res2 for r in res1]) and all([r in res1 for r in res2])


def test_clean_query():
    # Effectively also tests _clean_query_string()
    # Imbalanced/improper parentheses
    q1 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="() term ")
    assert q1._clean_query() == "term"
    q2 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="(term)(")
    assert q2._clean_query() == "(term)"
    q3 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="(term) AND (")
    assert q3._clean_query() == "(term)"
    q4 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="(term AND term2")
    assert q4._clean_query() == "(term AND term2)"
    q5 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="term AND term2)")
    assert q5._clean_query() == "(term AND term2)"
    q6 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="((((term AND term2")
    assert q6._clean_query() == "((((term AND term2))))"
    q7 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="term AND term2))))")
    assert q7._clean_query() == "((((term AND term2))))"

    # Correct trailing operators
    q8 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="term AND NOT term2 OR")
    assert q8._clean_query() == "term AND NOT term2"
    q9 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="term OR NOT term2 AND")
    assert q9._clean_query() == "term OR NOT term2"
    q10 = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="term OR term2 NOT")
    assert q10._clean_query() == "term OR term2"


def test_add_sort_internal():
    # Sort ascending by atomic number
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT, q="mdf.source_name:oqmd", advanced=True)
    q._add_sort('crystal_structure.number_of_atoms', True)
    res = q._ex_search(limit=1)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1

    # Sort descending by composition
    q._add_sort('material.composition', False)
    res = q._ex_search(limit=1)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1
    assert res[0]['material']['composition'].startswith('Zr')


# ***********************************************
# * Externals/user-facing
# ***********************************************

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


def test_match_field():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)

    # Basic usage
    f.match_field("mdf.source_name", "khazana_vasp")
    res1 = f.search()
    assert check_field(res1, "mdf.source_name", "khazana_vasp") == 0

    # Check that query clears
    assert f.current_query() == ""

    # Also checking check_field and no-op
    f.match_field("material.elements", "Al")
    f.match_field("", "")
    res2 = f.search()  # Enough so that we'd find at least 1 non-Al example
    assert check_field(res2, "material.elements", "Al") == 1


def test_exclude_field():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Basic usage
    f.exclude_field("material.elements", "Al")
    f.exclude_field("", "")
    f.match_field("mdf.source_name", "ab_initio_solute_database")
    f.match_field("mdf.resource_type", "record")
    res1 = f.search()
    assert check_field(res1, "material.elements", "Al") == -1


def test_add_sort_external():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Sort ascending by atomic number
    f.match_field("mdf.source_name", "oqmd")
    f.add_sort('crystal_structure.number_of_atoms', True)
    res = f.search(limit=1, reset_query=False)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1

    # Sort descending by composition, with multi-sort
    f.add_sort('material.composition', False)
    res = f.search(limit=1)
    assert res[0]['crystal_structure']['number_of_atoms'] == 1
    assert res[0]['material']['composition'].startswith('Zr')


def test_match_exists():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Basic usage
    f.match_exists("services.citrine")
    assert check_field(f.search(), "services.citrine", ".*") == 0


def test_match_not_exists():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Basic usage
    f.match_not_exists("services.citrine")
    assert check_field(f.search(), "services.citrine", ".*") == -1


def test_match_range():
    # Single-value use
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    f.match_range("material.elements", "Al", "Al")
    res1, info1 = f.search(info=True)
    assert check_field(res1, "material.elements", "Al") == 1

    res2, info2 = f.search("material.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] == info2["total_query_matches"]

    # Non-matching use, test inclusive
    f.match_range("material.elements", "Al", "Al", inclusive=False)
    assert f.search() == []

    # Actual range
    f.match_range("material.elements", "Al", "Cu")
    res4, info4 = f.search(info=True)
    assert info1["total_query_matches"] < info4["total_query_matches"]
    assert (check_field(res4, "material.elements", "Al") >= 0 or
            check_field(res4, "material.elements", "Cu") >= 0)

    # Nothing to match
    assert f.match_range("field", start=None, stop=None) == f


def test_exclude_range():
    # Single-value use
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    f.exclude_range("material.elements", "Am", "*")
    f.exclude_range("material.elements", "*", "Ak")
    f.match_field("material.elements", "*")
    res1, info1 = f.search(info=True)
    assert (check_field(res1, "material.elements", "Al") == 0 or
            check_field(res1, "material.elements", "Al") == 2)

    res2, info2 = f.search("material.elements:Al", advanced=True, info=True)
    assert info1["total_query_matches"] <= info2["total_query_matches"]

    # Non-matching use, test inclusive
    f.exclude_range("material.elements", "Am", "*")
    f.exclude_range("material.elements", "*", "Ak")
    f.exclude_range("material.elements", "Al", "Al", inclusive=False)
    f.match_field("material.elements", "*")
    res3, info3 = f.search(info=True)
    assert info1["total_query_matches"] == info3["total_query_matches"]

    # Nothing to match
    assert f.exclude_range("field", start=None, stop=None) == f


def test_exclusive_match():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    f.exclusive_match("material.elements", "Al")
    res1 = f.search()
    assert check_field(res1, "material.elements", "Al") == 0

    f.exclusive_match("material.elements", ["Al", "Cu"])
    res2 = f.search()
    assert check_field(res2, "material.elements", "Al") == 1
    assert check_field(res2, "material.elements", "Cu") == 1
    assert check_field(res2, "material.elements", "Cp") == -1
    assert check_field(res2, "material.elements", "Fe") == -1


def test_search(capsys):
    # Error on no query
    with pytest.raises(ValueError):
        f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
        f.search()

    # Return info if requested
    res2 = f.search("Al", info=False)
    assert isinstance(res2, list)
    assert isinstance(res2[0], dict)

    res3 = f.search("Al", info=True)
    assert isinstance(res3, tuple)
    assert isinstance(res3[0], list)
    assert isinstance(res3[0][0], dict)
    assert isinstance(res3[1], dict)

    # Check limit
    res4 = f.match_term("Al").search(limit=3)
    assert len(res4) == 3

    # Check reset_query
    f.match_field("mdf.source_name", "ta_melting")
    res5 = f.search(reset_query=False)
    res6 = f.search()
    assert all([r in res6 for r in res5]) and all([r in res5 for r in res6])

    # Check default index
    f2 = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    assert (f2.match_term("data").search(limit=1, info=True)[1]["index_uuid"] ==
            mdf_toolbox.translate_index(INDEX))


def test_reset_query():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Term will return results
    f.match_field("material.elements", "Al")
    f.reset_query()

    # Specifying no query will raise an error
    with pytest.raises(ValueError):
        assert f.search() == []


def test_current_query():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    # Query.clean_query() is already tested, just need to check basic functionality
    f.match_field("field", "value")
    assert f.current_query() == "(field:value)"


def test_show_fields():
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    res1 = f.show_fields("top")
    assert "mdf" in res1.keys()
    res2 = f.show_fields(block="mdf")
    assert "mdf.source_name" in res2.keys()
    res3 = f.show_fields()
    assert "dc.creators.creatorName" in res3.keys()


def test_anonymous(capsys):
    f = SearchHelper(INDEX, anonymous=True)
    # Test search
    assert len(f.search("mdf.source_name:ab_initio_solute_database",
                        advanced=True, limit=300)) == 300
