"""
Module initialization for ga_engine_tools package.

- Checks environment/GeoAnalytics compatibility on import.
- Verifies that the 'geoanalytics' package is installed.
- Ensures Native Geometry priority by avoiding conflicting SQL imports.

Raises:
    ImportError: If 'geoanalytics' is not installed.
"""

import importlib.util
import logging

# Set up package-level logging
_logger = logging.getLogger(__name__)

# 1. Check if the 'geoanalytics' package is installed
if importlib.util.find_spec("geoanalytics") is None:
    raise ImportError(
        "The 'geoanalytics' package is not installed. "
        "Please install it to use geoanalytics features: "
        "https://developers.arcgis.com/geoanalytics/install/databricks/"
    )
    
# 2. Import the compatibility check function from utils
# We use relative imports to stay within the package structure
from .utils import (
    check_environment_compatibility,
    initialize_gis_connection,
    reproject_df
)

# 3. Execute the check immediately upon package import
# This provides the user with immediate feedback on DBR/GAE versions
try:
    check_environment_compatibility()
except Exception as e:
    _logger.warning(f"Compatibility check could not be completed: {e}")

# 4. Import submodules for convenient access
# Ensure these submodules DO NOT 'import geoanalytics.sql.functions as ST'
from . import agol_data_export
from . import agol_data_import
from . import utils

__all__ = [
    'check_environment_compatibility',
    'initialize_gis_connection',
    'reproject_df',
    'agol_data_export',
    'agol_data_import',
    'utils'
]
