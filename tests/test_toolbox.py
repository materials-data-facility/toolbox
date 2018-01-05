from copy import deepcopy
import os
import json
import pytest
import globus_sdk
from mdf_toolbox import toolbox


credentials = {
    "app_name": "MDF_Forge",
    "services": []
    }


def test_login(capsys):
    # Login works
    creds1 = deepcopy(credentials)
    creds1["services"] = ["search"]
    res1 = toolbox.login(creds1)
    assert type(res1) is dict
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)

    # Test other services
    creds2 = deepcopy(credentials)
    creds2["services"] = ["search_ingest", "mdf", "transfer", "publish"]
    res2 = toolbox.login(creds2)
    assert isinstance(res2.get("search_ingest"), globus_sdk.SearchClient)
    assert isinstance(res2.get("transfer"), globus_sdk.TransferClient)
    assert isinstance(res2.get("mdf"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("publish"), toolbox.DataPublicationClient)

    # Test nothing
    creds3 = deepcopy(credentials)
    assert toolbox.login(creds3) == {}

    # Error on bad creds
    with pytest.raises(ValueError):
        toolbox.login("nope")
    with pytest.raises(ValueError):
        toolbox.login()

    # Error on bad services
    creds4 = deepcopy(credentials)
    creds4["services"] = ["garbage", "invalid"]
    assert toolbox.login(creds4) == {}
    out, err = capsys.readouterr()
    assert "Unknown or invalid service: 'garbage'." in out
    assert "Unknown or invalid service: 'invalid'." in out

    # TODO: Test user input prompt


def test_confidential_login():
    # TODO
    pass


def test_anonymous_login(capsys):
    # Valid services work
    res1 = toolbox.anonymous_login(["transfer", "search", "publish"])
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)
    assert isinstance(res1.get("transfer"), globus_sdk.TransferClient)
    assert isinstance(res1.get("publish"), toolbox.DataPublicationClient)

    # Single service works
    res2 = toolbox.anonymous_login("search")
    assert isinstance(res2.get("search"), globus_sdk.SearchClient)

    # Auth-only services don't work
    assert toolbox.anonymous_login(["search_ingest", "mdf"]) == {}
    out, err = capsys.readouterr()
    assert "Error: Service 'search_ingest' requires authentication." in out
    assert "Error: Service 'mdf' requires authentication." in out

    # Bad services don't work
    assert toolbox.anonymous_login(["garbage", "invalid"]) == {}
    out, err = capsys.readouterr()
    assert "Unknown or invalid service: 'garbage'." in out
    assert "Unknown or invalid service: 'invalid'." in out


def test_find_files():
    root = os.path.join(os.path.dirname(__file__), "testing_files")
    # Get everything
    res1 = list(toolbox.find_files(root))
    fn1 = [r["filename"] for r in res1]
    assert all([name in fn1 for name in [
                "2_toolbox.txt",
                "3_toolbox_3.txt",
                "4toolbox4.txt",
                "6_toolbox.dat",
                "toolbox_1.txt",
                "toolbox_5.csv",
                "txttoolbox.csv",
                "toolbox_compressed.tar"
                ]])
    # Check paths and no_root_paths
    for res in res1:
        assert res["path"] == os.path.join(root, res["no_root_path"])
        assert os.path.isfile(os.path.join(res["path"], res["filename"]))

    # Get everything (by regex)
    res2 = list(toolbox.find_files(root, "toolbox"))
    fn2 = [r["filename"] for r in res2]
    correct2 = [
        "2_toolbox.txt",
        "3_toolbox_3.txt",
        "4toolbox4.txt",
        "6_toolbox.dat",
        "toolbox_1.txt",
        "toolbox_5.csv",
        "txttoolbox.csv",
        "toolbox_compressed.tar"
        ]
    fn2.sort()
    correct2.sort()
    assert fn2 == correct2

    # Get only txt files
    res3 = list(toolbox.find_files(root, "txt$"))
    fn3 = [r["filename"] for r in res3]
    correct3 = [
        "2_toolbox.txt",
        "3_toolbox_3.txt",
        "4toolbox4.txt",
        "toolbox_1.txt"]
    fn3.sort()
    correct3.sort()
    assert fn3 == correct3

    # Test error
    with pytest.raises(ValueError):
        next(toolbox.find_files("/this/is/not/a/valid/path"))


def test_uncompress_tree():
    root = os.path.join(os.path.dirname(__file__), "testing_files")
    toolbox.uncompress_tree(root)
    path = os.path.join(root, "toolbox_more", "tlbx_uncompressed.txt")
    assert os.path.isfile(path)
    os.remove(path)


def test_format_gmeta():
    # Simple GMetaEntry
    md1 = {
        "mdf": {
            "acl": ["public"],
            "landing_page": "https://example.com"
            }
        }
    # More complex GMetaEntry
    md2 = {
        "mdf": {
                "title": "test",
                "acl": ["public"],
                "source_name": "source name",
                "citation": ["abc"],
                "landing_page": "http://www.globus.org",
                "data_contact": {
                    "given_name": "Test",
                    "family_name": "McTesterson",
                    "full_name": "Test McTesterson",
                    "email": "test@example.com"
                },
                "data_contributor": [{
                    "given_name": "Test",
                    "family_name": "McTesterson",
                    "full_name": "Test McTesterson",
                    "email": "test@example.com"
                }],
                "ingest_date": "Jan 1, 2017",
                "metadata_version": "1.1",
                "mdf_id": "1",
                "resource_type": "dataset"
        },
        "dc": {},
        "misc": {}
    }

    # Format both
    gme1 = toolbox.format_gmeta(md1)
    assert gme1 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "https://example.com",
            "visible_to": ["public"],
            "content": {
                "mdf": {
                    "landing_page": "https://example.com"
                }
            }
        }
    gme2 = toolbox.format_gmeta(md2)
    assert gme2 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "http://www.globus.org",
            "visible_to": ["public"],
            "content": {
                "mdf": {
                    "title": "test",
                    "source_name": "source name",
                    "citation": ["abc"],
                    "landing_page": "http://www.globus.org",
                    "data_contact": {
                        "given_name": "Test",
                        "family_name": "McTesterson",
                        "full_name": "Test McTesterson",
                        "email": "test@example.com"
                    },
                    "data_contributor": [{
                        "given_name": "Test",
                        "family_name": "McTesterson",
                        "full_name": "Test McTesterson",
                        "email": "test@example.com"
                    }],
                    "ingest_date": "Jan 1, 2017",
                    "metadata_version": "1.1",
                    "mdf_id": "1",
                    "resource_type": "dataset"
                },
                "dc": {},
                "misc": {}
            }
        }
    # Format into GMetaList
    gmlist = toolbox.format_gmeta([gme1, gme2])
    assert gmlist == {
        "@datatype": "GIngest",
        "@version": "2016-11-09",
        "ingest_type": "GMetaList",
        "ingest_data": {
            "@datatype": "GMetaList",
            "@version": "2016-11-09",
            "gmeta": [gme1, gme2]
            }
        }

    # Error if bad type
    with pytest.raises(TypeError):
        toolbox.format_gmeta(1)


def test_gmeta_pop():
    class TestResponse():
        status_code = 200
        headers = {
            "Content-Type": "json"
            }
        data = {
            '@datatype': 'GSearchResult',
            '@version': '2016-11-09',
            'count': 11,
            'gmeta': [{
                '@datatype': 'GMetaResult',
                '@version': '2016-11-09',
                'content': [{
                    'mdf': {
                        'links': {
                            'landing_page':
                                'https://data.materialsdatafacility.org/test/test_fetch.txt',
                            'txt': {
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
                                "path": "/test/test_fetch.txt"
                                }
                            }
                        }
                    }, {
                    'mdf': {
                        'links': {
                            'landing_page':
                                'https://data.materialsdatafacility.org/test/test_fetch.txt',
                            'txt': {
                                "globus_endpoint": "82f1b5c6-6e9b-11e5-ba47-22000b92c6ec",
                                "http_host": "https://data.materialsdatafacility.org",
                                "path": "/test/test_fetch.txt"
                                }
                            }
                        }
                    }],
                'subject': 'https://data.materialsdatafacility.org/test/test_fetch.txt',
                }],
            'offset': 0,
            'total': 22
            }
        text = json.dumps(data)

        def json(self):
            return self.data
    ghttp = globus_sdk.GlobusHTTPResponse(TestResponse())
    popped = toolbox.gmeta_pop(ghttp)
    assert popped == [{
            'mdf': {
                'links': {
                    'landing_page': 'https://data.materialsdatafacility.org/test/test_fetch.txt',
                    'txt': {
                        'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                        'http_host': 'https://data.materialsdatafacility.org',
                        'path': '/test/test_fetch.txt'
                    }
                }
            }
        }, {
            'mdf': {
                'links': {
                    'landing_page': 'https://data.materialsdatafacility.org/test/test_fetch.txt',
                    'txt': {
                        'globus_endpoint': '82f1b5c6-6e9b-11e5-ba47-22000b92c6ec',
                        'http_host': 'https://data.materialsdatafacility.org',
                        'path': '/test/test_fetch.txt'
                    }
                }
            }
        }]
    info_pop = toolbox.gmeta_pop(ghttp, info=True)
    print(info_pop)
    assert info_pop == (popped, {'total_query_matches': 22})

    # String loading
    str_gmeta = json.dumps({
                    "gmeta": [{
                        "content": [
                            {"test1": "test1"},
                            {"test2": "test2"}
                        ]
                    },
                        {
                        "content": [
                            {"test3": "test3"},
                            {"test4": "test4"}
                        ]
                    }
                    ]})
    assert toolbox.gmeta_pop(str_gmeta) == [
                            {"test1": "test1"},
                            {"test2": "test2"},
                            {"test3": "test3"},
                            {"test4": "test4"}
                        ]

    # Error on bad data
    with pytest.raises(TypeError):
        toolbox.gmeta_pop(1)


def test_quick_transfer():
    # TODO
    pass


def test_get_local_ep():
    # TODO
    pass


def test_dict_merge():
    base = {
        "base_key": "base",
        "both_key": "base",
        "level2": {
            "base_key": "base",
            "both_key": "base",
            "level3": {
                "base_key": "base",
                "both_key": "base"
            }
        }
    }
    add = {
        "both_key": "add",
        "add_key": "add",
        "level2": {
            "both_key": "add",
            "add_key": "add",
            "level3": {
                "both_key": "add",
                "add_key": "add",
                "level4": {
                    "add_key": "add"
                }
            }
        }
    }
    merged = {
        "base_key": "base",
        "both_key": "base",
        "add_key": "add",
        "level2": {
            "base_key": "base",
            "both_key": "base",
            "add_key": "add",
            "level3": {
                "base_key": "base",
                "both_key": "base",
                "add_key": "add",
                "level4": {
                    "add_key": "add"
                }
            }
        }
    }
    # Proper use
    assert toolbox.dict_merge(base, add) == merged
    assert toolbox.dict_merge({}, {}) == {}

    # Check errors
    with pytest.raises(TypeError):
        toolbox.dict_merge(1, {})
    with pytest.raises(TypeError):
        toolbox.dict_merge({}, "a")
    with pytest.raises(TypeError):
        toolbox.dict_merge([], [])


def test_DataPublicationClient():
    # TODO
    pass
