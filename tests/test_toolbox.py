from copy import deepcopy
import json
import os
import shutil

from globus_nexus_client import NexusClient
import globus_sdk
import mdf_toolbox
import pytest

credentials = {
    "app_name": "MDF_Forge",
    "services": []
    }


def test_login(capsys, monkeypatch):
    # Login works
    creds1 = deepcopy(credentials)
    creds1["services"] = ["search"]
    res1 = mdf_toolbox.login(creds1)
    assert type(res1) is dict
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)

    # Test other services
    creds2 = deepcopy(credentials)
    creds2["services"] = ["search_ingest", "transfer", "data_mdf", "connect",
                          "petrel", "publish", "groups"]
    res2 = mdf_toolbox.login(creds2)
    print(res2)
    assert isinstance(res2.get("search_ingest"), globus_sdk.SearchClient)
    assert isinstance(res2.get("transfer"), globus_sdk.TransferClient)
    assert isinstance(res2.get("data_mdf"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("connect"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("petrel"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("publish"), mdf_toolbox.DataPublicationClient)
    assert isinstance(res2.get("groups"), NexusClient)

    creds3 = deepcopy(credentials)
    creds3["services"] = "mdf_connect"
    res3 = mdf_toolbox.login(creds3)
    assert isinstance(res3.get("mdf_connect"), mdf_toolbox.MDFConnectClient)

    # Test fetching previous tokens
    creds = deepcopy(credentials)
    assert mdf_toolbox.login(creds).get("petrel_https_server")

    # Error on bad creds
    with pytest.raises(ValueError):
        mdf_toolbox.login("nope")

    # TODO: Test user input prompt
    # monkeypatch.setattr(mdf_toolbox, "input", (lambda x=None: "invalid"))
    # with pytest.raises(ValueError):
    #    mdf_toolbox.login()


def test_confidential_login(capsys):
    # Load creds
    with open(os.path.expanduser("~/.mdf/credentials/client_credentials.json")) as f:
        creds = json.load(f)

    # Single services, different cases
    assert isinstance(mdf_toolbox.confidential_login(creds, services="transfer")["transfer"],
                      globus_sdk.TransferClient)
    assert isinstance(mdf_toolbox.confidential_login(creds, services=["search"])["search"],
                      globus_sdk.SearchClient)
    # Manual scope set
    assert isinstance(mdf_toolbox.confidential_login(
                                    creds,
                                    services="urn:globus:auth:scope:transfer.api.globus.org:all"
                                    )["urn:globus:auth:scope:transfer.api.globus.org:all"],
                      globus_sdk.ClientCredentialsAuthorizer)
    # make_clients=False
    assert isinstance(mdf_toolbox.confidential_login(
                                    creds, services="transfer", make_clients=False)["transfer"],
                      globus_sdk.ClientCredentialsAuthorizer)
    # Arg creds
    assert isinstance(mdf_toolbox.confidential_login(client_id=creds["client_id"],
                                                     client_secret=creds["client_secret"],
                                                     services=["publish"])["publish"],
                      mdf_toolbox.DataPublicationClient)
    # No client available
    assert isinstance(mdf_toolbox.confidential_login(creds, services="petrel")["petrel"],
                      globus_sdk.ClientCredentialsAuthorizer)

    # No scope
    assert mdf_toolbox.confidential_login(creds) == {}
    # Bad scope
    assert mdf_toolbox.confidential_login(creds, services="invalid") == {}
    out, err = capsys.readouterr()
    assert "Error: Cannot create authorizer for scope 'invalid'" in out


def test_anonymous_login(capsys):
    # Valid services work
    res1 = mdf_toolbox.anonymous_login(["transfer", "search", "publish", "groups"])
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)
    assert isinstance(res1.get("transfer"), globus_sdk.TransferClient)
    assert isinstance(res1.get("publish"), mdf_toolbox.DataPublicationClient)
    assert isinstance(res1.get("groups"), NexusClient)

    # Single service works
    res2 = mdf_toolbox.anonymous_login("search")
    assert isinstance(res2.get("search"), globus_sdk.SearchClient)

    # Bad services don't work
    assert mdf_toolbox.anonymous_login(["garbage", "invalid"]) == {}
    out, err = capsys.readouterr()
    assert "Error: No known client for 'garbage' service." in out
    assert "Error: No known client for 'invalid' service." in out


def test_uncompress_tree():
    root = os.path.join(os.path.dirname(__file__), "testing_files")
    # Basic test, should extract tar and nested tar, but not delete anything
    mdf_toolbox.uncompress_tree(root)
    lv1_txt = os.path.join(root, "toolbox_more", "toolbox_compressed", "tlbx_uncompressed.txt")
    assert os.path.isfile(lv1_txt)
    lv2_txt = os.path.join(root, "toolbox_more", "toolbox_compressed", "toolbox_nested",
                           "tlbx_uncompressed2.txt")
    assert os.path.isfile(lv2_txt)
    nested_tar = os.path.join(root, "toolbox_more", "toolbox_compressed", "toolbox_nested.tar")
    assert os.path.isfile(nested_tar)

    # Test deleting extracted archive
    shutil.rmtree(os.path.join(root, "toolbox_more", "toolbox_compressed", "toolbox_nested"))
    mdf_toolbox.uncompress_tree(os.path.join(root, "toolbox_more", "toolbox_compressed"),
                                delete_archives=True)
    assert os.path.isfile(lv2_txt)
    assert not os.path.isfile(nested_tar)

    # Clean up
    shutil.rmtree(os.path.join(root, "toolbox_more", "toolbox_compressed"))


def test_format_gmeta():
    # Simple GMetaEntry
    md1 = {
        "mdf": {
            "acl": ["public"],
            "mdf_id": "123",
            "data": "some"
            }
        }
    # More complex GMetaEntry
    md2 = {
        "mdf": {
                "title": "test",
                "source_name": "source name",
                "citation": ["abc"],
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
                "mdf_id": "123",
                "parent_id": "000",
                "resource_type": "dataset"
        },
        "dc": {},
        "misc": {}
    }

    # Format both
    gme1 = mdf_toolbox.format_gmeta(md1, md1["mdf"].pop("acl"), md1["mdf"]["mdf_id"])
    assert gme1 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "123",
            "visible_to": ["public"],
            "content": {
                "mdf": {
                    "mdf_id": "123",
                    "data": "some"
                }
            }
        }
    gme2 = mdf_toolbox.format_gmeta(md2, ["ABCD"], "https://example.com/123456")
    assert gme2 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "https://example.com/123456",
            "visible_to": ["ABCD"],
            "content": {
                "mdf": {
                    "title": "test",
                    "source_name": "source name",
                    "citation": ["abc"],
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
                    "mdf_id": "123",
                    "parent_id": "000",
                    "resource_type": "dataset"
                },
                "dc": {},
                "misc": {}
            }
        }
    # Format into GMetaList
    gmlist = mdf_toolbox.format_gmeta([gme1, gme2])
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
        mdf_toolbox.format_gmeta(1)


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
    popped = mdf_toolbox.gmeta_pop(ghttp)
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
    info_pop = mdf_toolbox.gmeta_pop(ghttp, info=True)
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
    assert mdf_toolbox.gmeta_pop(str_gmeta) == [
                            {"test1": "test1"},
                            {"test2": "test2"},
                            {"test3": "test3"},
                            {"test4": "test4"}
                        ]

    # Error on bad data
    with pytest.raises(TypeError):
        mdf_toolbox.gmeta_pop(1)


def test_translate_index():
    # Known index
    assert mdf_toolbox.translate_index("mdf") == "1a57bbe5-5272-477f-9d31-343b8258b7a5"
    # Unknown index
    assert mdf_toolbox.translate_index("frdr") == "9be6dd95-48f0-48bb-82aa-c6577a988775"
    # Invalid index
    assert mdf_toolbox.translate_index("invalid_index_not_real") == "invalid_index_not_real"


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
    assert mdf_toolbox.dict_merge(base, add) == merged
    assert mdf_toolbox.dict_merge({}, {}) == {}

    # Check errors
    with pytest.raises(TypeError):
        mdf_toolbox.dict_merge(1, {})
    with pytest.raises(TypeError):
        mdf_toolbox.dict_merge({}, "a")
    with pytest.raises(TypeError):
        mdf_toolbox.dict_merge([], [])


def test_insensitive_comparison():
    # Correct results:
    # dbase == d1 always
    # dbase == d2 iff string_insensitive=True
    # dbase == d3 iff type_insensitive=True
    # dbase == d4 never (extra dict key)
    # dbase == d5 never (extra list item)
    # dbase == d6 never (float not equal)
    dbase = {
        "aaa": ["a", "zzz", 4, 5, "QQzz"],
        "ccc": "AAAABBBBCCCC",
        "bbb": 50.00000000000,
        "www": (1, 2, 9, 4, 5, "F")
    }
    d1 = {
        "bbb": 50.0,
        "aaa": ["a", 5, 4, "zzz", "QQzz"],
        "www": (2, 1, 9, 5, "F", 4),
        "ccc": "AAAABBBBCCCC"
    }
    d2 = {
        "aaa": ["a", "zzz", 4, 5, "zzqq"],
        "ccc": "aaaaBBBBCCCC",
        "bbb": 50.00000000000,
        "www": (1, 2, 9, 4, 5, "f")
    }
    d3 = {
        "aaa": ("a", "zzz", 4, 5, "QQzz"),
        "ccc": "AAAABBBBCCCC",
        "bbb": 50.00000000000,
        "www": [1, 2, 9, 4, 5, "F"]
    }
    d4 = {
        "aaa": ["a", "zzz", 4, 5, "QQzz"],
        "ccc": "AAAABBBBCCCC",
        "bbb": 50.00000000000,
        "www": (1, 2, 9, 4, 5, "F"),
        "zzz": "abc"
    }
    d5 = {
        "aaa": ["a", "zzz", 4, 5, "QQzz", "zzz"],
        "ccc": "AAAABBBBCCCC",
        "bbb": 50.00000000000,
        "www": (1, 2, 9, 4, 5, "F")
    }
    d6 = {
        "aaa": ["a", "zzz", 4, 5, "QQzz"],
        "ccc": "AAAABBBBCCCC",
        "bbb": 50.1,
        "www": (1, 2, 9, 4, 5, "F")
    }

    assert mdf_toolbox.insensitive_comparison(dbase, d1) is True
    assert mdf_toolbox.insensitive_comparison(dbase, d2) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d2, string_insensitive=True) is True
    assert mdf_toolbox.insensitive_comparison(dbase, d3) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d3, type_insensitive=True) is True
    assert mdf_toolbox.insensitive_comparison(dbase, d4) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d4, string_insensitive=True,
                                              type_insensitive=True) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d5) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d5, string_insensitive=True,
                                              type_insensitive=True) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d6) is False
    assert mdf_toolbox.insensitive_comparison(dbase, d6, string_insensitive=True,
                                              type_insensitive=True) is False


def test_DataPublicationClient():
    # TODO
    pass
