import yaml

def test_yaml_load():
    with open('detectors.yaml','r',encoding='utf-8') as f:
        data = yaml.safe_load(f)
    assert isinstance(data, list) and len(data) >= 1
