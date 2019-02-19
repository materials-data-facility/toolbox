import mdf_toolbox
import pytest
from globus_sdk import SearchAPIError
from mdf_toolbox.search_helper import SearchHelper


# Manually logging in for SearchHelper testing
SEARCH_CLIENT = mdf_toolbox.login(credentials={"app_name": "MDF_Forge",
                                               "services": ["search"]})["search"]
INDEX = "mdf"


# ***********************************************
# * Static functions
# ***********************************************

def test_clean_query_string():
    #TODO
    pass


def test_validate_query():
    #TODO
    pass


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


def test_aggregate(capsys):
    #TODO
    pass
    '''
    q = SearchHelper(INDEX, search_client=SEARCH_CLIENT, advanced=True)
    # Error on no query
    with pytest.raises(ValueError) as excinfo:
        q.aggregate("mdf")
    assert "Query not set" in str(excinfo.value)

    # Basic aggregation
    q._SearchHelper__query["q"] = "mdf.source_name:nist_xps_db"
    res1 = q.aggregate("mdf")
    assert len(res1) > 10000
    assert isinstance(res1[0], dict)

    # Multi-dataset aggregation
    q._SearchHelper__query["q"] = "(mdf.source_name:nist_xps_db OR mdf.source_name:khazana_vasp)"
    res2 = q.aggregate(index="mdf")
    assert len(res2) > 10000
    assert len(res2) > len(res1)

    # Unnecessary aggregation fallback to .search()
    # Check success in Coveralls
    q._SearchHelper__query["q"] = "mdf.source_name:khazana_vasp"
    assert len(q.aggregate("mdf")) < 10000
    '''


def test_chaining():
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


def test_clean_query():
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


def test_add_sort():
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

