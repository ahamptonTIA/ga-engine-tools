import os
import time
import re
import warnings
import logging
import pkg_resources
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from packaging.version import parse as parse_version

# 1. ESRI Core (Keep for AGOL connectivity and data loading)
import geoanalytics
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection as FLC

# CRITICAL: Do NOT import geoanalytics.sql.functions as ST
# This prevents ESRI from hijacking the Databricks Native ST_ functions.

# 2. Databricks & Spark Native
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.window import Window

# ------------------------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

# Compatibility mapping remains the same
GAE_DBR_COMPATIBILITY = {
    parse_version("1.1"): (parse_version("7.3"), parse_version("12.9")),
    parse_version("1.2"): (parse_version("7.3"), parse_version("13.9")),
    parse_version("1.3"): (parse_version("9.1"), parse_version("14.9")),
    parse_version("1.4"): (parse_version("9.1"), parse_version("15.9")),
    parse_version("1.5"): (parse_version("11.3"), parse_version("15.9")),
    parse_version("1.6"): (parse_version("11.3"), parse_version("16.9")),
    parse_version("1.7"): (parse_version("12.2"), parse_version("17.9")),
}

# ------------------------------------------------------------------------------------------

def check_environment_compatibility():
    """Checks compatibility between DBR and GAE."""
    current_dbr_version = None
    current_gae_version = None

    dbr_spark_version_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None)
    if dbr_spark_version_tag:
        dbr_version_parts = dbr_spark_version_tag.split(".")[0:2]
        current_dbr_version = parse_version(".".join(dbr_version_parts))

    if geoanalytics:
        try:
            gae_version_str = pkg_resources.get_distribution("geoanalytics").version
            current_gae_version = parse_version(".".join(gae_version_str.split('.')[:2]))
        except Exception: pass

    if current_dbr_version and current_gae_version:
        if current_gae_version in GAE_DBR_COMPATIBILITY:
            min_dbr, max_dbr = GAE_DBR_COMPATIBILITY[current_gae_version]
            if not (min_dbr <= current_dbr_version <= max_dbr):
                print(f"Warning: GAE v{current_gae_version} mismatch with DBR {current_dbr_version}.")
            else:
                print(f"Compatibility check passed: GAE {current_gae_version} on DBR {current_dbr_version}.")
    else:
        print("Compatibility check skipped: Version info missing.")

# ------------------------------------------------------------------------------------------

def geoanalytics_unregister():
    """Unregister the GIS configuration from geoanalytics."""
    try:
        geoanalytics.unregister_gis("GIS")
    except Exception:
        pass

# ------------------------------------------------------------------------------------------

def initialize_gis_connection(agol_inst, user, pswd, print_status=True):
    """Initialize and register GIS for geoanalytics (required for feature-service reader)."""
    try:
        if print_status: print("\t-Setting ArcGIS Online Connections")
        gis = GIS(url=agol_inst, username=user, password=pswd)
        
        geoanalytics_unregister()
        geoanalytics.register_gis("GIS", agol_inst, username=user, password=pswd)
        
        if print_status: print("\t-Successfully registered GIS connection for geoanalytics.")
        return gis
    except Exception as e:
        if print_status: print(f"Failed to connect: {e}")
        return None

# ------------------------------------------------------------------------------------------

def get_spatial_reference_info(df):
    """Extract spatial reference info using the .st accessor (distributed by GAE)."""
    geom_col = df.st.get_geometry_field()
    spatial_ref = df.st.get_spatial_reference()

    info_dict = {
        'Geometry Column': geom_col,
        'Spatial Reference WKID': spatial_ref.srid,
        'Spatial Reference WKT': spatial_ref.wkt,
        'Spatial Reference is Projected': spatial_ref.is_projected,
        'Spatial Reference Units': spatial_ref.unit,
    }

    match = re.search(r'GEOGCS\[\s*"([^"]+)"', spatial_ref.wkt)
    info_dict['Spatial Reference Name'] = match.group(1) if match else None
    return info_dict

# ------------------------------------------------------------------------------------------

def reproject_df(df, out_cs=3857):
    """
    Reproject using Databricks Native ST_Transform.
    Bypasses ESRI conflicts by using SQL expressions.
    """
    # 1. Identify the geometry column using the ESRI distributed engine reader
    geom_col = df.st.get_geometry_field()
    if not geom_col:
        print('DataFrame has no defined geometry/spatial reference')
        return df

    # 2. Check current SRID
    sr = df.st.get_spatial_reference()
    if sr.srid != out_cs:
        print(f'Transforming {sr.srid} -> {out_cs} using Native Spark SQL...')
        
        # We use F.expr to force the SQL engine to resolve ST_Transform.
        # This ensures the Databricks native engine takes priority over the ESRI JAR.
        df = df.withColumn(
            geom_col, 
            F.expr(f"ST_Transform({geom_col}, {out_cs})")
        )
        
        print('\t-Transformation complete.')
    else:
        print(f'No transformation required! Already: {out_cs}')

    return df
