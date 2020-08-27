import pytest
import json


@pytest.fixture(scope='session')
def test_params():
    with open('endpointconf.json', 'r') as f:
        params_dict = json.load(f)
    return params_dict
