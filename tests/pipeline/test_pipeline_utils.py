from dplutils.pipeline.utils import dict_from_coord


def test_dict_from_coord():
    assert dict_from_coord('a.b.c', 'value') == {'a': {'b': {'c': 'value'}}}
