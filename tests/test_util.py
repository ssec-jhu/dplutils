from dplutils import __project__, __version__
from dplutils.util import find_package_location, find_repo_location


def test_version():
    assert __version__


def test_project():
    assert __project__


def test_find_package_location():
    assert find_package_location()


def test_find_repo_location():
    assert find_repo_location()
