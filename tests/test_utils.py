"""Test for utility functions"""

from mdf_toolbox.utils import rectify_path


def test_rectify():
    assert rectify_path('C:\\Users\\') == '/c/Users'
    assert rectify_path('/users/test') == '/users/test'
