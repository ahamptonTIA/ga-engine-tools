"""
ga_engine_tools: Utilities for ArcGIS GeoAnalytics and PySpark.

This package provides tools for managing ArcGIS GIS connections and 
transferring large-scale data between Databricks and ArcGIS Online.
"""

import importlib.util
import logging
from typing import List

# Package Metadata
__version__ = "1.0.0"
__author__ = "ahamptonTIA"

# Package-level logging
# Following library best practices, we attach a NullHandler so the 
# host application (Databricks) controls the log output.
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

def _verify_geoanalytics_installed():
    """Verify the presence of geoanalytics before package initialization."""
    if importlib.util.find_spec("geoanalytics") is None:
        raise ImportError(
            "The 'geoanalytics' package is required but was not found. "
            "Refer to ESRI documentation for installation instructions: "
            "https://developers.arcgis.com/geoanalytics/install/databricks/"
        )

# Execute verification
_verify_geoanalytics_installed()

# Import core utilities to the top-level namespace for ease of use
from .utils import (
    check_environment_compatibility,
    initialize_gis_connection,
    reproject_df
)

# Expose submodules
from . import agol_data_export
from . import agol_data_import
from . import utils

# Immediate compatibility check upon import
try:
    check_environment_compatibility()
except Exception as e:
    logger.debug(f"Initial compatibility check deferred: {e}")

__all__: List[str] = [
    'check_environment_compatibility',
    'initialize_gis_connection',
    'reproject_df',
    'agol_data_export',
    'agol_data_import',
    'utils'
]