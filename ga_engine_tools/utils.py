import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import warnings
import logging
from packaging.version import parse as parse_version
import pkg_resources
import geoanalytics
from geoanalytics.sql import functions as ST
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection as FLC
import pandas as pd
import re

# Import spark from databricks.sdk.runtime for Databricks notebooks
from databricks.sdk.runtime import spark

from pyspark.sql import functions as F

# ------------------------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

# Compatibility mapping for GeoAnalytics Engine and Databricks Runtimes.
# Maps GAE major.minor versions to (min_dbr, max_dbr) tuples (inclusive).
GAE_DBR_COMPATIBILITY = {
    parse_version("1.1"): (parse_version("7.3"), parse_version("12.1")),
    parse_version("1.2"): (parse_version("7.3"), parse_version("13.2")),
    parse_version("1.3"): (parse_version("9.1"), parse_version("14.2")),
    parse_version("1.4"): (parse_version("9.1"), parse_version("15.2")),
    parse_version("1.5"): (parse_version("11.3"), parse_version("15.4")),
    parse_version("1.6"): (parse_version("11.3"), parse_version("16.4")),
    parse_version("1.7"): (parse_version("12.2"), parse_version("17.9")),
    # Update as new versions are validated:
    # https://developers.arcgis.com/geoanalytics/install/databricks/
}

# ------------------------------------------------------------------------------------------

def check_environment_compatibility():
    """
    Check compatibility between Databricks Runtime and ArcGIS GeoAnalytics Engine.

    Attempts to detect the current Databricks Runtime version and GeoAnalytics Engine
    version, then compares them against a predefined compatibility matrix. Issues
    warnings if a mismatch is detected, and provides guidance to ESRI documentation.

    Returns
    -------
    None

    Notes
    -----
    - Prints or warns about compatibility issues.
    - Uses ESRI's official documentation for reference.
    """
    current_dbr_version = None
    current_gae_version = None

    # Get Databricks Runtime version from Spark config
    dbr_spark_version_tag = spark.conf.get(
        "spark.databricks.clusterUsageTags.sparkVersion", None
    )
    if dbr_spark_version_tag:
        dbr_version_parts = dbr_spark_version_tag.split(".")[0:2]
        current_dbr_version = parse_version(".".join(dbr_version_parts))

    # Get GeoAnalytics Engine version
    if geoanalytics:
        try:
            gae_version_str = pkg_resources.get_distribution("geoanalytics").version
            current_gae_version = parse_version(
                ".".join(gae_version_str.split('.')[:2])
            )
            try:
                geoanalytics.auth_info().show()
            except Exception:
                pass
        except Exception:
            pass

    # Perform compatibility check
    if current_dbr_version and current_gae_version:
        if current_gae_version in GAE_DBR_COMPATIBILITY:
            min_dbr, max_dbr = GAE_DBR_COMPATIBILITY[current_gae_version]
            if not (min_dbr <= current_dbr_version <= max_dbr):
                warnings.warn(
                    f"\n--- GeoAnalytics Engine Compatibility Warning ---\n"
                    f"ArcGIS GeoAnalytics Engine v{current_gae_version} is typically "
                    f"compatible with Databricks Runtimes from {min_dbr} to {max_dbr}.\n"
                    f"The current Databricks Runtime is {current_dbr_version}.\n"
                    "This mismatch may lead to unexpected behavior or errors. "
                    "Please refer to ESRI's official GeoAnalytics Engine documentation:\n"
                    "https://developers.arcgis.com/geoanalytics/install/databricks/\n"
                    "---------------------------------------------------\n"
                )
            else:
                print(
                    f"GeoAnalytics Engine ({current_gae_version}) and Databricks Runtime "
                    f"({current_dbr_version}) compatibility check passed."
                )
        else:
            print(
                f"GeoAnalytics Engine compatibility check: The GeoAnalytics Engine "
                f"version ({current_gae_version}) is not explicitly listed in this "
                "package's known compatibility matrix. Please ensure the Databricks Runtime "
                "is compatible with the GeoAnalytics Engine version by checking "
                "ESRI's official documentation:\n"
                "https://developers.arcgis.com/geoanalytics/install/databricks/"
            )
    else:
        msg_parts = []
        if not current_dbr_version:
            msg_parts.append("Databricks Runtime version could not be determined.")
        if not current_gae_version:
            msg_parts.append("ArcGIS GeoAnalytics Engine version could not be determined.")
        if msg_parts:
            print(
                "GeoAnalytics Engine compatibility check skipped: "
                f"{' '.join(msg_parts)} This check runs only when GeoAnalytics Engine "
                "is initialized. Please ensure the environment meets the recommended "
                "specifications by consulting ESRI's documentation."
            )

# ------------------------------------------------------------------------------------------

def geoanalytics_unregister():
    """
    Unregister the GIS configuration named 'GIS' from geoanalytics.

    Silently passes if unregistration fails or if no configuration exists.

    Returns
    -------
    None

    Notes
    -----
    - Useful for resetting or cleaning up GIS connections.
    """
    try:
        geoanalytics.unregister_gis("GIS")
    except Exception:
        pass

# ------------------------------------------------------------------------------------------

def initialize_gis_connection(agol_inst, user, pswd, print_status=True):
    """
    Initialize and register a connection to ArcGIS GIS for geoanalytics.

    Parameters
    ----------
    agol_inst : str
        URL of the ArcGIS Online instance or ArcGIS Enterprise portal.
    user : str
        Username for authentication.
    pswd : str
        Password for authentication.
    print_status : bool, optional
        If True, prints status messages. Default is True.

    Returns
    -------
    GIS or None
        The GIS object if connection and registration are successful, otherwise None.

    Notes
    -----
    - Unregisters any existing geoanalytics GIS configuration before registering a new one.
    - Prints status messages if `print_status` is True.
    """
    gis = None
    try:
        if print_status:
            print("\t\t-Setting/resetting ArcGIS Online Connections")
        gis = GIS(url=agol_inst, username=user, password=pswd)
        if print_status:
            print("\t-Successfully connected to ArcGIS GIS.")

        try:
            geoanalytics_unregister()
            if print_status:
                print("\t\t\t-Successfully unregistered existing geoanalytics configuration.")
        except Exception as e:
            if print_status:
                print(f"\t\t\t-Warning: Could not unregister existing geoanalytics configuration: {e}")

        geoanalytics.register_gis("GIS", agol_inst, username=user, password=pswd)
        if print_status:
            print("\t-Successfully registered GIS connection for geoanalytics.")

        return gis

    except Exception as e:
        if print_status:
            print(f"Failed to connect to ArcGIS GIS or register for geoanalytics: {e}")
        return None

# ------------------------------------------------------------------------------------------

def get_spatial_reference_info(df):
    """
    Extract spatial reference information from a Spark DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame containing spatial data.

    Returns
    -------
    dict
        Dictionary with spatial reference details:
            - 'Geometry Column' : str
            - 'Spatial Reference WKID' : int
            - 'Spatial Reference WKT' : str
            - 'Spatial Reference is Projected' : bool
            - 'Spatial Reference Units' : str
            - 'Spatial Reference Name' : str or None

    Notes
    -----
    - The spatial reference name is parsed from the WKT if available.
    """
    geom_col = df.st.get_geometry_field()
    spatial_ref = df.st.get_spatial_reference()

    info_dict = {
        'Geometry Column': geom_col,
        'Spatial Reference WKID': spatial_ref.srid,
        'Spatial Reference WKT': spatial_ref.wkt,
        'Spatial Reference is Projected': spatial_ref.is_projected,
        'Spatial Reference Units': spatial_ref.unit,
    }

    wkt = spatial_ref.wkt
    match = re.search(r'GEOGCS\[\s*"([^"]+)"', wkt)
    info_dict['Spatial Reference Name'] = match.group(1) if match else None

    return info_dict

# ------------------------------------------------------------------------------------------

def reproject_df(df, out_cs=3857):
    """
    Reproject a PySpark DataFrame to a new coordinate system.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        DataFrame to reproject.
    out_cs : int, optional
        SRID of the output coordinate system. Default is 3857.

    Returns
    -------
    pyspark.sql.DataFrame
        The reprojected DataFrame.

    Notes
    -----
    - If the DataFrame is already in the target SRID, no transformation is performed.
    - Prints transformation details.
    """
    geom_col = df.st.get_geometry_field()
    if not geom_col:
        print('DataFrame has no defined geometry/spatial reference')
        return df

    sr = df.st.get_spatial_reference()
    if sr.srid != out_cs:
        print('Transforming Spatial Reference')
        print('\t\t-Input Projection:')
        print(f'\t\t-SRID: {sr.srid}')
        print(f'\t\t-Projected: {sr.is_projected}')
        print(f'\t\t-Unit: {sr.unit}')

        df = df.withColumn(geom_col, ST.transform(geom_col, out_cs))

        sr = df.st.get_spatial_reference()
        print('\n\t-Transformed To\n')
        print('\t\t-Output Projection:')
        print(f'\t\t-SRID: {sr.srid}')
        print(f'\t\t-Projected: {sr.is_projected}')
        print(f'\t\t-Unit: {sr.unit}')
    else:
        print(f'No transformation required! DataFrame is already set to: {out_cs}')

    return df

# ------------------------------------------------------------------------------------------
