import os
import time
import logging
from datetime import datetime # timedelta removed as unused
# from concurrent.futures import ThreadPoolExecutor, TimeoutError # Moved/Checked below

from .utils import *

import geoanalytics
# CHANGED: Alias to GAE to avoid collision with Databricks native ST functions
from geoanalytics.sql import functions as GAE 

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection as FLC
import pandas as pd

# Import spark from databricks.sdk.runtime for Databricks notebooks
from databricks.sdk.runtime import spark

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    DateType, BinaryType, TimestampType, IntegerType
)

# ------------------------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------

def set_pyspark_schema_from_feature_service(
    input_df: DataFrame,
    layer_url: str,
    gis_object: GIS
) -> DataFrame:
    """
    Casts a Spark DataFrame to match the schema of an ArcGIS Feature Service layer.
    """
    geom_types = [
        'PointUDT', 'LineStringUDT', 'PolygonUDT',
        'MultiPointUDT', 'MultiLineStringUDT', 'MultiPolygonUDT'
    ]
    try:
        feature_layer = FeatureLayer(layer_url, gis=gis_object)
        feature_layer_schema = feature_layer.properties.fields

        type_mapping = {
            "esriFieldTypeSmallInteger": LongType(),
            "esriFieldTypeInteger": LongType(),
            "esriFieldTypeSingle": DoubleType(),
            "esriFieldTypeDouble": DoubleType(),
            "esriFieldTypeString": StringType(),
            "esriFieldTypeDate": DateType(),
            "esriFieldTypeOID": LongType(),
            "esriFieldTypeGlobalID": StringType(),
            "esriFieldTypeGUID": StringType(),
            "esriFieldTypeBlob": BinaryType(),
            "esriFieldTypeRaster": BinaryType(),
            "esriFieldTypeGeometry": BinaryType(),
            "esriFieldTypeXML": StringType()
        }

        spark_fields = []
        for field in feature_layer_schema:
            field_name = field['name']
            arcgis_type = field['type']
            is_nullable = field.get('nullable', True)
            spark_type = type_mapping.get(arcgis_type, StringType())

            if field_name in input_df.columns:
                spark_fields.append(
                    StructField(field_name, spark_type, is_nullable)
                )

        input_df = input_df.select(
            [
                F.col(field.name).cast(field.dataType)
                if field.dataType not in geom_types else F.col(field.name)
                for field in StructType(spark_fields)
            ] + [
                F.col(c) for c in input_df.columns
                if c not in [field.name for field in StructType(spark_fields)]
            ]
        )
        return input_df

    except Exception as e:
        _logger.error(f"An error occurred during schema casting: {e}")
        raise

# ------------------------------------------------------------------------------------------

def _feature_service_log_(
    batch_id_value,
    target_svc_name,
    target_server_url,
    batch_by_column,
    layer_id,
    write_mode,
    user,
    status,
    message,
    logTable,
    attempt_count
):
    """
    Logs the status of a batch processing attempt to the specified logTable.
    """
    if status not in ["Failed", "Success", "Timeout"]:
        raise ValueError("Status must be 'Timeout', 'Failed' or 'Success'.")

    log_message = message if message else (
        "Complete" if status == "Success" else "Unknown error"
    )

    if logTable:
        log_entry = [(
            str(batch_id_value), target_svc_name, target_server_url,
            batch_by_column, layer_id, write_mode, user,
            datetime.now(), status, log_message, attempt_count
        )]
        log_df = spark.createDataFrame(
            log_entry,
            [
                "batch", "target_svc_name", "target_server_url",
                "batch_by_column", "layer_id", "write_mode", "user",
                "load_timestamp", "status", "message", "retry_attempts"
            ]
        )
        try:
            if spark.catalog.tableExists(logTable):
                log_df.createOrReplaceTempView("log_temp_view")
                # Using dynamic SQL to handle the Merge
                spark.sql(f"""
                    MERGE INTO {logTable} AS target
                    USING log_temp_view AS source
                    ON target.batch = source.batch AND target.target_svc_name = source.target_svc_name
                    WHEN MATCHED THEN
                        UPDATE SET
                            target.load_timestamp = source.load_timestamp,
                            target.status = source.status,
                            target.message = source.message,
                            target.retry_attempts = source.retry_attempts
                    WHEN NOT MATCHED THEN
                        INSERT *
                """)
            else:
                log_df.write.mode("append").saveAsTable(logTable)
        except Exception as e:
            _logger.error(f"Error saving log entry to table '{logTable}': {e}")
    
    print(f"Batch '{batch_id_value}' {status.lower()}: {log_message} (Attempt {attempt_count})")

# ------------------------------------------------------------------------------------------

def check_create_feature_service(
    gis_object: GIS,
    service_name: str,
    folder: str = None,
    create_if_not_exists: bool = False,
    owner_username: str = None,
    summary: str = None,
    tags: list = None
):
    """
    Checks if a Feature Service exists; optionally creates a new empty Feature Service.
    """
    try:
        search_query = f"title:\"{service_name}\" AND type:\"Feature Service\""
        if owner_username:
            search_query += f" AND owner:\"{owner_username}\""

        feature_services = gis_object.content.search(query=search_query, item_type="Feature Service")

        found_service = next((s for s in feature_services if s.title == service_name), None)

        if found_service:
            layer_info_list = []
            flc = FLC.fromitem(found_service)
            for layer in flc.layers + flc.tables:
                layer_info_list.append({
                    'name': layer.properties.get('name'),
                    'id': layer.properties.get('id')
                })
            return True, found_service.url, found_service.id, layer_info_list
        
        elif create_if_not_exists:
            created_service = gis_object.content.create_service(
                name=service_name, folder=folder,
                description=summary or f"Service for {service_name}",
                tags=tags or ["automated_creation"]
            )
            return True, created_service.url, created_service.id, []
        
        return False, None, None, []
    except Exception as e:
        _logger.error(f"Error during service check/creation: {e}")
        return False, None, None, []

# ------------------------------------------------------------------------------------------

def write_df_in_chunks(
    df_to_write: DataFrame,
    is_first_batch: bool,
    initial_write_mode: str,
    parallelism: int,
    service_url: str,
    layer_name: str,
    retry_delay_seconds: int = 30,
    attempt_count: int = 1,
    max_chunk_size: int = 500000
) -> tuple:
    """
    Write data to the feature service in chunks.
    """
    total_rows = df_to_write.count()
    if total_rows == 0:
        return True, is_first_batch, initial_write_mode

    temp_col = '__temp_chunk_col__'
    window_spec = Window.orderBy(F.monotonically_increasing_id())

    df_to_write = df_to_write.withColumn(
        temp_col,
        F.floor((F.row_number().over(window_spec) - 1) / max_chunk_size).cast("int") + 1
    )

    max_chunk_num = df_to_write.select(F.col(temp_col)).distinct().count()

    for chunk_num in range(1, max_chunk_num + 1):
        chunk_df = df_to_write.filter(F.col(temp_col) == chunk_num).drop(temp_col)
        effective_write_mode = initial_write_mode if (is_first_batch and chunk_num == 1) else "append"

        try:
            chunk_df.write.format("feature-service") \
                .option("gis", "GIS") \
                .option("maxParallelism", parallelism) \
                .option("serviceUrl", service_url) \
                .option("layerName", layer_name) \
                .mode(effective_write_mode) \
                .save()
            is_first_batch = False
        except Exception as e:
            return e

    return True, is_first_batch, "append"

# ------------------------------------------------------------------------------------------

def update_arcgis_feature_service(
    input_df: DataFrame,
    target_svc_name: str,
    target_server_url: str,
    batch_by_column_name: str,
    layer_name: str = None,
    write_mode: str = "overwrite",
    agol_inst: str = None,
    user: str = None,
    pswd: str = None,
    max_total_retries: int = 5,
    max_chunk_size: int = 500000,
    retry_delay_seconds: int = 30,
    maxParallelism: int = 12,
    logTable: str = None,
    timeout_minutes: int = 15,
    folder: str = None,
    service_summary: str = None,
    service_tags: list = None
) -> bool:
    """
    Main orchestrator for updating ArcGIS Feature Services from Spark.
    """
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    
    global gis
    gis = initialize_gis_connection(agol_inst, user, pswd)

    attempt_count = 1
    success = False

    while attempt_count <= max_total_retries:
        # Check existence and get current IDs
        svc_exists, service_url, item_id, layer_list = check_create_feature_service(
            gis, target_svc_name, folder, True, user, service_summary, service_tags
        )
        
        # Logic to determine which batches to process
        batches = [row[0] for row in input_df.select(batch_by_column_name).distinct().collect()]
        
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                for idx, b_id in enumerate(batches):
                    subset_df = input_df.filter(F.col(batch_by_column_name) == b_id)
                    future = executor.submit(
                        write_df_in_chunks, subset_df, (idx==0), write_mode, 
                        maxParallelism, service_url, layer_name, retry_delay_seconds, 
                        attempt_count, max_chunk_size
                    )
                    
                    res = future.result(timeout=timeout_minutes * 60)
                    if isinstance(res, Exception): raise res
                    
                    _feature_service_log_(b_id, target_svc_name, target_server_url, 
                                          batch_by_column_name, item_id, write_mode, 
                                          user, "Success", "Batch Complete", logTable, attempt_count)
            success = True
            break
        except Exception as e:
            _logger.error(f"Attempt {attempt_count} failed: {e}")
            time.sleep(retry_delay_seconds)
            attempt_count += 1

    return success
