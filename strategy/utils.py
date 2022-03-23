"""
    file with helper functions
"""

import os
from pathlib import Path
import yaml
import structlog


ROOT_DIR = Path(__file__).parent.parent
CONFIG_PATH = os.path.join(ROOT_DIR, 'configs/config.yml')
DATA_DIR = os.path.join(ROOT_DIR, 'data')

log = structlog.get_logger()


class ConfigParser:
    """
        Parse yml config configs/config.yml to python dict
    """
    def __init__(self, config_path):
        self.path = config_path

        with open(self.path, 'r') as stream:
            self.config = yaml.safe_load(stream)

