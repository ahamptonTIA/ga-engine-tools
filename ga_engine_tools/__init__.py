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

# Set package version
__version__ = "1.0.0"

# Set up package-level logging with a NullHandler
_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())

# 1. Check if the 'geoanalytics' package is installed
if importlib.util.find_spec("geoanalytics") is None:
    raise ImportError(
        "The 'geoanalytics' package is not installed. "
        "Please install it to use geoanalytics features: "
        "https://developers.arcgis.com/geoanalytics/install/databricks/"
    )

# 2. Import core functions from utils for top-level access
from .utils import (
    check_environment_compatibility,
    initialize_gis_connection,
    reproject_df
)

# 3. Execute compatibility check immediately upon package import
try:
    check_environment_compatibility()
except Exception as e:
    _logger.warning(f"Compatibility check could not be completed: {e}")

# 4. Import submodules to expose them under the package namespace
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