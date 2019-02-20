def test_aggregate_internal(capsys):
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




def test_aggregate_external():
    # Test that aggregate uses the current query properly
    # And returns results
    # And respects the reset_query arg
    f = SearchHelper(INDEX, search_client=SEARCH_CLIENT)
    f.match_field("mdf.source_name", "nist_xps_db")
    res1 = f.aggregate(reset_query=False, index="mdf")
    assert len(res1) > 10000
    assert check_field(res1, "mdf.source_name", "nist_xps_db") == 0
    res2 = f.aggregate()
    assert len(res2) == len(res1)
    assert check_field(res2, "mdf.source_name", "nist_xps_db") == 0



