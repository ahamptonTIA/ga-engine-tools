"""
Module initialization for ga_engine_tools package.

- Checks environment/GeoAnalytics compatibility on import.
- Imports key submodules for convenient access.
- Verifies that the 'geoanalytics' package is installed.

Raises:
    ImportError: If 'geoanalytics' is not installed.
"""

import importlib.util

# Check if the 'geoanalytics' package is installed
if importlib.util.find_spec("geoanalytics") is None:
    raise ImportError(
        "The 'geoanalytics' package is not installed. "
        "Please install it to use geoanalytics features."
    )
    
# Import the compatibility check function from utils
from .utils import check_environment_compatibility

# Execute the check when the package is imported to provide immediate feedback
check_environment_compatibility()

# Import other modules in the package for easier access
from . import agol_data_export
from . import agol_data_import
from . import utils
