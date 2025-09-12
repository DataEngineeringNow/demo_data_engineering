import yaml
import os
from pathlib import Path

class YamlIncludeLoader(yaml.SafeLoader):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super().__init__(stream)

def construct_include(loader, node):
    filename = os.path.join(loader._root, loader.construct_scalar(node))
    with open(filename, 'r') as f:
        return yaml.load(f, YamlIncludeLoader)

# Register the !include constructor
YamlIncludeLoader.add_constructor('!include', construct_include)

def load_config():
    # Load main config
    config_path = os.path.join(os.path.dirname(__file__), 'pipeline_config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.load(f, Loader=YamlIncludeLoader)
    
    # Merge credentials into oltp_db config
    if 'oltp_db' in config and 'credentials' in config['oltp_db']:
        credentials = config['oltp_db'].pop('credentials')
        config['oltp_db'].update(credentials)
    
    return config

# Load the configuration
PIPELINE_CONFIG = load_config()
