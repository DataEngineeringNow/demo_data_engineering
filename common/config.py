import yaml
import os

# Load pipeline config from YAML or JSON if needed
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'pipeline_config.yaml')

with open(CONFIG_PATH, 'r') as f:
    PIPELINE_CONFIG = yaml.safe_load(f)
