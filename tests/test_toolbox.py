from copy import deepcopy
import json
import os
import shutil

from globus_nexus_client import NexusClient
import globus_sdk
import mdf_toolbox
import pytest
from unittest import mock

on_github = os.getenv('ON_GITHUB') is not None

def test_login():
    if on_github: return True
    
    # Login works
    # Impersonate Forge
    res1 = mdf_toolbox.login(services="search", app_name="MDF_Forge",
                             client_id="b2b437c4-17c1-4e4b-8f15-e9783e1312d7")
    assert type(res1) is dict
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)

    # Test other services
    # Use default "unknown app"
    # TODO: "groups" cannot be tested without whitelisting app
    res2 = mdf_toolbox.login(services=["search_ingest", "transfer", "data_mdf", "mdf_connect",
                                       "petrel"])
    assert isinstance(res2.get("search_ingest"), globus_sdk.SearchClient)
    assert isinstance(res2.get("transfer"), globus_sdk.TransferClient)
    assert isinstance(res2.get("data_mdf"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("mdf_connect"), globus_sdk.RefreshTokenAuthorizer)
    assert isinstance(res2.get("petrel"), globus_sdk.RefreshTokenAuthorizer)
    # assert isinstance(res2.get("groups"), NexusClient)


def test_confidential_login(capsys):
    if on_github: return True
    # Load creds
    with open(os.path.expanduser("~/.client_credentials.json")) as f:
        creds = json.load(f)

    # Single services, different cases
    assert isinstance(mdf_toolbox.confidential_login(services="transfer", **creds)["transfer"],
                      globus_sdk.TransferClient)
    assert isinstance(mdf_toolbox.confidential_login(services=["search"], **creds)["search"],
                      globus_sdk.SearchClient)
    # Manual scope set
    assert isinstance(mdf_toolbox.confidential_login(
                                    services="urn:globus:auth:scope:transfer.api.globus.org:all",
                                    **creds)["urn:globus:auth:scope:transfer.api.globus.org:all"],
                      globus_sdk.TransferClient)
    # make_clients=False
    assert isinstance(mdf_toolbox.confidential_login(
                                    services="transfer", make_clients=False, **creds)["transfer"],
                      globus_sdk.ClientCredentialsAuthorizer)
    assert isinstance(mdf_toolbox.confidential_login(
                                    services="urn:globus:auth:scope:transfer.api.globus.org:all",
                                    make_clients=False,
                                    **creds)["urn:globus:auth:scope:transfer.api.globus.org:all"],
                      globus_sdk.ClientCredentialsAuthorizer)
    # No client available
    assert isinstance(mdf_toolbox.confidential_login(services="petrel", **creds)["petrel"],
                      globus_sdk.ClientCredentialsAuthorizer)

    # Bad scope
    assert mdf_toolbox.confidential_login(services="invalid", **creds) == {}
    out, err = capsys.readouterr()
    assert "Error: Cannot create authorizer for scope 'invalid'" in out


def test_anonymous_login(capsys):
    if on_github: return True
    # Valid services work
    res1 = mdf_toolbox.anonymous_login(["transfer", "search", "publish", "groups"])
    assert isinstance(res1.get("search"), globus_sdk.SearchClient)
    assert isinstance(res1.get("transfer"), globus_sdk.TransferClient)
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
    if on_github: return True
    root = os.path.join(os.path.dirname(__file__), "testing_files")
    # Basic test, should extract tar and nested tar, but not delete anything
    # Also should error on known-bad-weird archive
    res = mdf_toolbox.uncompress_tree(root)
    assert res["success"]
    assert res["num_extracted"] == 2
    assert res["files_errored"] == [os.path.join(root, "toolbox_more", "toolbox_error.tar.gz")]
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
    shutil.rmtree(os.path.join(root, "toolbox_more", "toolbox_error.tar/"))


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
    gme2 = mdf_toolbox.format_gmeta(md2, ["abcd"], "https://example.com/123456")
    assert gme2 == {
            "@datatype": "GMetaEntry",
            "@version": "2016-11-09",
            "subject": "https://example.com/123456",
            "visible_to": ["urn:globus:auth:identity:abcd", "urn:globus:groups:id:abcd"],
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
    ghttp = globus_sdk.GlobusHTTPResponse(TestResponse(), client=mock.Mock())
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
    # Invalid index
    assert mdf_toolbox.translate_index("invalid_index_not_real") == "invalid_index_not_real"


def test_quick_transfer():
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
                "both_key": "base",
                "mismatch_key": "string"
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
                "mismatch_key": 10,
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
                "mismatch_key": "string",
                "level4": {
                    "add_key": "add"
                }
            }
        }
    }
    b_list = {
        "list_field": ["base"]
    }
    a_list = {
        "list_field": ["add"]
    }
    m_list = {
        "list_field": ["base", "add"]
    }
    a_list_bad = {
        "list_field": "foo"
    }
    # Proper use
    old_base = deepcopy(base)
    old_add = deepcopy(add)
    assert mdf_toolbox.dict_merge(base, add) == merged
    # Originals should be unchanged
    assert base == old_base
    assert add == old_add

    # Test list appending
    # No appending
    assert mdf_toolbox.dict_merge(b_list, a_list, append_lists=False) == b_list
    # With appending
    assert mdf_toolbox.dict_merge(b_list, a_list, append_lists=True) == m_list
    # With mismatched data types
    assert mdf_toolbox.dict_merge(b_list, a_list_bad, append_lists=False) == b_list
    assert mdf_toolbox.dict_merge(b_list, a_list_bad, append_lists=True) == b_list

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
        "ccc": "aaaaBB BBCCC\tC\n",
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


def test_translate_json():
    # Set up test dicts
    source_doc = {
        "dict1": {
            "field1": "value1",
            "field2": 2
        },
        "dict2": {
            "nested1": {
                "field1": True,
                "field3": "value3"
            }
        },
        "compost": "CN25",
        "na_val": "na"
    }
    mapping1 = {
        "custom": {
            "foo": "dict1.field1",
            "bar": "dict2.nested1.field1",
            "missing": "na_val"
        },
        "material": {
            "composition": "compost"
        }
    }
    mapping2 = {
        "custom.foo": "dict1.field1",
        "custom.bar": "dict2.nested1.field1",
        "custom.missing": "na_val",
        "material.composition": "compost"
    }
    correct_output = {
        "material": {
            "composition": "CN25"
        },
        "custom": {
            "foo": "value1",
            "bar": True,
            "missing": "na"
        }
    }
    no_na_output = {
        "material": {
            "composition": "CN25"
        },
        "custom": {
            "foo": "value1",
            "bar": True
        }
    }

    assert mdf_toolbox.translate_json(source_doc, mapping1) == correct_output
    assert mdf_toolbox.translate_json(source_doc, mapping2) == correct_output
    assert mdf_toolbox.translate_json(source_doc, mapping1, ["abcd"]) == correct_output
    assert mdf_toolbox.translate_json(source_doc, mapping1, ["na"]) == no_na_output
    assert mdf_toolbox.translate_json(source_doc, mapping1, "na") == no_na_output


def test_flatten_json():
    unflat_dict = {
        "key1": {
            "key2": {
                "key3": {
                    "key4": "value1"
                },
                "key5": "value2"
            },
            "key6": {
                "key7": 555,
                "key8": [1, {"list_flattened": "foo"}, "b"]
            }
        },
        "key9": "value3"
    }
    flat_dict = {
        "key1.key2.key3.key4": "value1",
        "key1.key2.key5": "value2",
        "key1.key6.key7": 555,
        "key1.key6.key8": [1, "b"],
        "key1.key6.key8.list_flattened": "foo",
        "key9": "value3"
    }
    assert mdf_toolbox.flatten_json(unflat_dict) == flat_dict


def test_posixify():
    assert mdf_toolbox.posixify_path('C:\\Users\\') == '/c/Users'
    assert mdf_toolbox.posixify_path('/users/test') == '/users/test'
