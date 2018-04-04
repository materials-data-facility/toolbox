import pytest
from mdf_toolbox import MDFConnectClient, CONNECT_SERVICE_LOC, CONNECT_DEV_LOC


def test_service_loc():
    mdf1 = MDFConnectClient()
    assert mdf1.service_loc == CONNECT_SERVICE_LOC
    mdf2 = MDFConnectClient(service_instance="prod")
    assert mdf2.service_loc == CONNECT_SERVICE_LOC
    mdf3 = MDFConnectClient(service_instance="dev")
    assert mdf3.service_loc == CONNECT_DEV_LOC


def test_create_dc_block():
    mdf = MDFConnectClient()
    # Full test, no lists
    mdf.create_dc_block(
        title="Connect Title",
        authors="Data Facility; Materials",
        affiliations="UChicago",
        publisher="Globus",
        publication_year="2017",
        resource_type="Dataset",
        description="This is a test",
        dataset_doi="10.555",
        related_dois="10.5555",
        other=5
    )
    assert mdf.dc == {
        'creators': [{
            'affiliations': ['UChicago'],
            'creatorName': 'Data Facility,  Materials',
            'familyName': 'Data Facility',
            'givenName': ' Materials'
        }],
        'descriptions': [{
            'description': 'This is a test',
            'descriptionType': 'Other'
        }],
        'identifier': {
            'identifier': '10.555',
            'identifierType': 'DOI'
        },
        'other': 5,
        'publicationYear': '2017',
        'publisher': 'Globus',
        'relatedIdentifiers': [{
            'relatedIdentifier': '10.5555',
            'relatedIdentifierType': 'DOI',
            'relationType': 'IsPartOf'
        }],
        'resourceType': {
            'resourceType': 'Dataset',
            'resourceTypeGeneral': 'Dataset'
        },
        'titles': [{
            'title': 'Connect Title'
        }]
    }
    # Full test, all lists
    mdf.create_dc_block(
        title=["Connect Title", "Other Title"],
        authors=["Data Facility; Materials", "Blaiszik, Ben", "Jonathon Gaff"],
        affiliations=["UChicago", "Argonne"],
        publisher="Globus",
        publication_year="2017",
        resource_type="Dataset",
        description="This is a test",
        dataset_doi="10.555",
        related_dois=["10.5555", "10.555-5555"],
        other=5,
        list_other=["a", "b"]
    )
    assert mdf.dc == {
        'creators': [{
                'affiliations': ['UChicago', 'Argonne'],
                'creatorName': 'Data Facility,  Materials',
                'familyName': 'Data Facility',
                'givenName': ' Materials'
            },
            {
                'affiliations': ['UChicago', 'Argonne'],
                'creatorName': 'Blaiszik,  Ben',
                'familyName': 'Blaiszik',
                'givenName': ' Ben'
            },
            {
                'affiliations': ['UChicago', 'Argonne'],
                'creatorName': 'Gaff, Jonathon',
                'familyName': 'Gaff',
                'givenName': 'Jonathon'
            }
        ],
        'descriptions': [{
            'description': 'This is a test',
            'descriptionType': 'Other'
        }],
        'identifier': {
            'identifier': '10.555',
            'identifierType': 'DOI'
        },
        'list_other': ['a', 'b'],
        'other': 5,
        'publicationYear': '2017',
        'publisher': 'Globus',
        'relatedIdentifiers': [{
                'relatedIdentifier': '10.5555',
                'relatedIdentifierType': 'DOI',
                'relationType': 'IsPartOf'
            },
            {
                'relatedIdentifier': '10.555-5555',
                'relatedIdentifierType': 'DOI',
                'relationType': 'IsPartOf'
            }
        ],
        'resourceType': {
            'resourceType': 'Dataset',
            'resourceTypeGeneral': 'Dataset'
        },
        'titles': [{
            'title': 'Connect Title'
        }, {
            'title': 'Other Title'
        }]
    }
    # Minimum test
    mdf.create_dc_block(
        title="Project One",
        authors=["Senior Programmer", "Programmer, Junior"]
    )
    assert mdf.dc == {
        'creators': [{
                'creatorName': 'Programmer, Senior',
                'familyName': 'Programmer',
                'givenName': 'Senior'
            },
            {
                'creatorName': 'Programmer,  Junior',
                'familyName': 'Programmer',
                'givenName': ' Junior'
            }
        ],
        'publicationYear': '2018',
        'publisher': 'Materials Data Facility',
        'resourceType': {
            'resourceType': 'Dataset',
            'resourceTypeGeneral': 'Dataset'
        },
        'titles': [{
            'title': 'Project One'
        }]
    }


def test_set_acl():
    mdf = MDFConnectClient()
    mdf.set_acl("12345abc")
    assert mdf.mdf == {
        "acl": ["12345abc"]
    }
    mdf.set_acl(["12345abc", "6789def"])
    assert mdf.mdf == {
        "acl": ["12345abc", "6789def"]
    }
    mdf.set_acl("public")
    assert mdf.mdf == {
        "acl": ["public"]
    }


def test_create_mrr_block():
    # TODO: Update after helper is helpful
    mdf = MDFConnectClient()
    mdf.create_mrr_block({"a": "b"})
    assert mdf.mrr == {"a": "b"}


def test_set_custom_block():
    mdf = MDFConnectClient()
    mdf.set_custom_block({"a": "b"})
    assert mdf.custom == {"a": "b"}


def test_data():
    mdf = MDFConnectClient()
    mdf.add_data("https://example.com/path/data.zip")
    assert mdf.data == ["https://example.com/path/data.zip"]
    mdf.add_data(["https://www.globus.org/app/transfer?123",
                  "globus://endpoint123/path/data.out"])
    assert mdf.data == ["https://example.com/path/data.zip",
                        "https://www.globus.org/app/transfer?123",
                        "globus://endpoint123/path/data.out"]
    mdf.clear_data()
    assert mdf.data == []


def test_index():
    mdf = MDFConnectClient()
    # Mapping only
    mdf.add_index("json", mapping={"materials.composition": "my_json.data.stuff.comp"})
    assert mdf.index == {
        "json": {
            "mapping": {"materials.composition": "my_json.data.stuff.comp"}
        }
    }
    # With delim/na
    mdf.add_index("csv", mapping={"materials.composition": "header1"},
                  delimiter="#", na_values="zero")
    assert mdf.index == {
        "json": {
            "mapping": {"materials.composition": "my_json.data.stuff.comp"}
        },
        "csv": {
            "mapping": {"materials.composition": "header1"},
            "delimiter": "#",
            "na_values": ["zero"]
        }
    }
    # Overwrite
    mdf.add_index("csv", mapping={"crystal_structure.space_group_number": "csv_header_2"})
    assert mdf.index == {
        "json": {
            "mapping": {"materials.composition": "my_json.data.stuff.comp"}
        },
        "csv": {
            "mapping": {"crystal_structure.space_group_number": "csv_header_2"}
        }
    }
    # Clear
    mdf.clear_index()
    assert mdf.index == {}


def test_services():
    mdf = MDFConnectClient()
    # No parameters
    mdf.add_services("citrine")
    assert mdf.services == {
        "citrine": True
    }
    # With parameters
    mdf.add_services("globus_publish", parameters={"collection_id": 5555})
    assert mdf.services == {
        "citrine": True,
        "globus_publish": {
            "collection_id": 5555
        }
    }
    # Cancelling
    mdf.add_services("citrine", False)
    assert mdf.services == {
        "citrine": False,
        "globus_publish": {
            "collection_id": 5555
        }
    }
    # Removing
    mdf.clear_services()
    assert mdf.services == {}


def test_set_test():
    mdf = MDFConnectClient()
    assert mdf.test is False
    mdf.set_test(True)
    assert mdf.test is True
    mdf.set_test(False)
    assert mdf.test is False
    mdf2 = MDFConnectClient(test=True)
    assert mdf2.test is True


def test_submit_dataset():
    # TODO
    pass


def test_check_status():
    # TODO
    pass
