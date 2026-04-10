"""
Utilities for exporting PySpark DataFrames to ArcGIS Online/Enterprise.

This module provides robust orchestration for publishing large datasets,
including schema alignment, batch processing, and Delta-based logging.
"""

import time
import logging
from datetime import datetime
from typing import Optional, List, Tuple, Any, Dict, Union
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# Third-party GIS (Esri core & GAE)
import geoanalytics
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection as FLC

# Databricks & Spark Native
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F, DataFrame, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    DateType, BinaryType
)

# Local Modules
from .utils import initialize_gis_connection

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Schema Management
# -----------------------------------------------------------------------------

def set_pyspark_schema_from_feature_service(
    input_df: DataFrame,
    layer_url: str,
    gis_object: GIS
) -> DataFrame:
    """
    Cast a Spark DataFrame to match an existing ArcGIS Feature Service schema.

    Parameters
    ----------
    input_df : pyspark.sql.DataFrame
        The source DataFrame to be cast.
    layer_url : str
        The REST URL of the target Feature Layer.
    gis_object : arcgis.gis.GIS
        Authenticated GIS connection.

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with column types aligned to the Feature Service.

    Raises
    ------
    Exception
        If the Feature Layer cannot be accessed or schema mapping fails.
    """
    # GAE Geometry types are typically represented as specialized UDTs or Binary
    geom_types = [
        'PointUDT', 'LineStringUDT', 'PolygonUDT',
        'MultiPointUDT', 'MultiLineStringUDT', 'MultiPolygonUDT'
    ]
    
    try:
        feature_layer = FeatureLayer(layer_url, gis=gis_object)
        feature_layer_schema = feature_layer.properties.fields

        # Map Esri REST API types to PySpark SQL types
        type_map = {
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
            f_name = field['name']
            f_arc_type = field['type']
            f_nullable = field.get('nullable', True)
            f_spark_type = type_map.get(f_arc_type, StringType())

            if f_name in input_df.columns:
                spark_fields.append(
                    StructField(f_name, f_spark_type, f_nullable)
                )

        schema_fields = StructType(spark_fields)
        schema_names = [f.name for f in schema_fields]

        # Apply casting, skipping complex geometry types that require GAE handling
        select_list = [
            F.col(f.name).cast(f.dataType)
            if f.dataType not in geom_types else F.col(f.name)
            for f in schema_fields
        ]

        # Preserve any internal columns not present in the target service
        select_list += [
            F.col(c) for c in input_df.columns if c not in schema_names
        ]

        return input_df.select(select_list)

    except Exception as e:
        logger.error(f"Failed to align PySpark schema to Feature Service: {e}")
        raise

# -----------------------------------------------------------------------------
# Logging & Orchestration Helpers
# -----------------------------------------------------------------------------

def _feature_service_log(
    batch_id: Any,
    target_svc: str,
    target_url: str,
    batch_col: str,
    layer_id: str,
    mode: str,
    user: str,
    status: str,
    message: Optional[str],
    log_table: Optional[str],
    attempt: int
) -> None:
    """
    Log the status of a batch processing attempt to a Delta table.

    Parameters
    ----------
    batch_id : Any
        Identifier for the specific data chunk/batch.
    target_svc : str
        Name of the target ArcGIS Feature Service.
    target_url : str
        REST URL of the service.
    batch_col : str
        The column name used for partitioning the export.
    layer_id : str
        The AGOL Item ID for the service.
    mode : str
        Write mode (overwrite/append).
    user : str
        The user performing the operation.
    status : str
        One of "Success", "Failed", or "Timeout".
    message : str, optional
        Custom error or status message.
    log_table : str, optional
        The Delta table path (e.g., catalog.schema.table) to write logs to.
    attempt : int
        The retry attempt number.
    """
    valid_statuses = ["Failed", "Success", "Timeout"]
    if status not in valid_statuses:
        raise ValueError(f"Status must be one of {valid_statuses}")

    log_msg = message if message else ("Complete" if status == "Success" else "Error")

    if log_table:
        entry = [(
            str(batch_id), target_svc, target_url, batch_col,
            layer_id, mode, user, datetime.now(), status, log_msg, attempt
        )]
        
        cols = [
            "batch", "target_svc_name", "target_server_url", "batch_by_column",
            "layer_id", "write_mode", "user", "load_timestamp", "status",
            "message", "retry_attempts"
        ]
        
        log_df = spark.createDataFrame(entry, cols)

        try:
            if spark.catalog.tableExists(log_table):
                log_df.createOrReplaceTempView("log_temp_view")
                spark.sql(f"""
                    MERGE INTO {log_table} AS target
                    USING log_temp_view AS source
                    ON target.batch = source.batch 
                    AND target.target_svc_name = source.target_svc_name
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
                log_df.write.mode("append").saveAsTable(log_table)
        except Exception as e:
            logger.error(f"Failed to write log to Delta table {log_table}: {e}")

    # Professional console feedback for Databricks notebooks
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch '{batch_id}' {status.lower()}: {log_msg}")


def check_create_feature_service(
    gis_object: GIS,
    service_name: str,
    folder: Optional[str] = None,
    create_if_not_exists: bool = False,
    owner: Optional[str] = None,
    summary: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> Tuple[bool, Optional[str], Optional[str], List[Dict[str, Any]]]:
    """
    Verify existence of a Feature Service or optionally create a new one.

    Parameters
    ----------
    gis_object : arcgis.gis.GIS
        Authenticated GIS connection.
    service_name : str
        Title of the service to find/create.
    folder : str, optional
        AGOL folder to place the service in if created.
    create_if_not_exists : bool, default False
        Whether to instantiate a new service if not found.
    owner : str, optional
        Filter search by owner.
    summary : str, optional
        Service description.
    tags : List[str], optional
        Tags to apply to the service.

    Returns
    -------
    Tuple[bool, str, str, List[Dict]]
        (Exists, URL, Item ID, Layer Information)
    """
    try:
        query = f"title:\"{service_name}\" AND type:\"Feature Service\""
        if owner:
            query += f" AND owner:\"{owner}\""

        items = gis_object.content.search(query=query, item_type="Feature Service")
        found = next((s for s in items if s.title == service_name), None)

        if found:
            layer_info = []
            flc = FLC.fromitem(found)
            for lyr in flc.layers + flc.tables:
                layer_info.append({
                    'name': lyr.properties.get('name'),
                    'id': lyr.properties.get('id')
                })
            return True, found.url, found.id, layer_info

        elif create_if_not_exists:
            logger.info(f"Creating new Feature Service: {service_name}")
            svc = gis_object.content.create_service(
                name=service_name,
                folder=folder,
                description=summary or f"Automated service for {service_name}",
                tags=tags or ["automated_export"]
            )
            return True, svc.url, svc.id, []

        return False, None, None, []

    except Exception as e:
        logger.error(f"Error during Feature Service check/creation: {e}")
        return False, None, None, []

# -----------------------------------------------------------------------------
# Data Transfer Logic
# -----------------------------------------------------------------------------

def write_df_in_chunks(
    df_to_write: DataFrame,
    is_first_batch: bool,
    initial_mode: str,
    parallelism: int,
    service_url: str,
    layer_name: Optional[str],
    max_chunk: int = 500000
) -> Union[Tuple[bool, bool, str], Exception]:
    """
    Write a Spark DataFrame to a Feature Service using internal row-based chunking.

    Parameters
    ----------
    df_to_write : pyspark.sql.DataFrame
        The data subset to be written.
    is_first_batch : bool
        True if this is the very first part of the export.
    initial_mode : str
        The user-defined write mode (overwrite/append).
    parallelism : int
        Maximum number of concurrent Spark tasks for the write.
    service_url : str
        The Feature Service REST URL.
    layer_name : str, optional
        Target layer name within the service.
    max_chunk : int, default 500000
        Number of rows per sub-chunk to prevent REST API timeouts.

    Returns
    -------
    Union[Tuple[bool, bool, str], Exception]
        (Success Status, Next Batch 'is_first' Flag, Next Mode) or Exception object.
    """
    total = df_to_write.count()
    if total == 0:
        return True, is_first_batch, initial_mode

    tmp_col = '__chunk_id__'
    # Monotonically increasing ID is used to create a predictable row order for chunking
    win = Window.orderBy(F.monotonically_increasing_id())

    df_to_write = df_to_write.withColumn(
        tmp_col,
        F.floor((F.row_number().over(win) - 1) / max_chunk).cast("int") + 1
    )

    max_idx = df_to_write.select(tmp_col).distinct().count()

    for idx in range(1, max_idx + 1):
        chunk = df_to_write.filter(F.col(tmp_col) == idx).drop(tmp_col)
        
        # Only the absolute first chunk of the entire job respects 'overwrite'
        current_mode = initial_mode if (is_first_batch and idx == 1) else "append"

        try:
            chunk.write.format("feature-service") \
                .option("gis", "GIS") \
                .option("maxParallelism", parallelism) \
                .option("serviceUrl", service_url) \
                .option("layerName", layer_name) \
                .mode(current_mode) \
                .save()
            is_first_batch = False
        except Exception as e:
            return e

    return True, is_first_batch, "append"


def update_arcgis_feature_service(
    input_df: DataFrame,
    target_svc_name: str,
    target_server_url: str,
    batch_col: str,
    layer_name: Optional[str] = None,
    write_mode: str = "overwrite",
    agol_inst: Optional[str] = None,
    user: Optional[str] = None,
    pswd: Optional[str] = None,
    max_retries: int = 5,
    max_chunk: int = 500000,
    retry_delay: int = 30,
    parallelism: int = 12,
    log_table: Optional[str] = None,
    timeout_mins: int = 15,
    folder: Optional[str] = None,
    summary: Optional[str] = None,
    tags: Optional[List[str]] = None
) -> bool:
    """
    Orchestrate the high-volume export of a Spark DataFrame to ArcGIS Online.

    Uses a tiered strategy: 
    1. Group data by `batch_col` (e.g., State or Date).
    2. Sub-chunk each batch by `max_chunk` to manage HTTP request sizes.
    3. Handles retries and timeouts via ThreadPoolExecutor.

    Parameters
    ----------
    input_df : pyspark.sql.DataFrame
        Source spatial data.
    target_svc_name : str
        Name of the Feature Service in AGOL.
    target_server_url : str
        Base REST URL for the server.
    batch_col : str
        Column used to split the DataFrame into manageable processing batches.
    layer_name : str, optional
        Target layer within the service.
    write_mode : str, default "overwrite"
        Standard Spark write modes.
    agol_inst : str, optional
        URL of the ArcGIS instance.
    user : str, optional
        Username for authentication.
    pswd : str, optional
        Password for authentication.
    max_retries : int, default 5
        Number of times to retry a failed batch.
    max_chunk : int, default 500000
        Max rows per individual API write call.
    retry_delay : int, default 30
        Seconds to wait between retries.
    parallelism : int, default 12
        Spark parallelism for the GAE write operation.
    log_table : str, optional
        Path to a Delta table for audit logging.
    timeout_mins : int, default 15
        Hard timeout for a single batch processing operation.
    folder : str, optional
        AGOL folder to house the service.
    summary : str, optional
        Service metadata description.
    tags : List[str], optional
        Service metadata tags.

    Returns
    -------
    bool
        True if all batches were successfully processed.
    """
    gis = initialize_gis_connection(agol_inst, user, pswd)
    if not gis:
        return False

    attempt = 1
    success = False

    while attempt <= max_retries:
        # Step 1: Ensure the service and layers exist
        exists, svc_url, item_id, layers = check_create_feature_service(
            gis, target_svc_name, folder, True, user, summary, tags
        )
        
        # Step 2: Get unique batches to process
        batches = [row[0] for row in input_df.select(batch_col).distinct().collect()]
        
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                is_first = True
                for b_id in batches:
                    subset = input_df.filter(F.col(batch_col) == b_id)
                    
                    # Submit work with a hard timeout to prevent hung Spark jobs
                    future = executor.submit(
                        write_df_in_chunks, subset, is_first, write_mode,
                        parallelism, svc_url, layer_name, max_chunk
                    )
                    
                    try:
                        res = future.result(timeout=timeout_mins * 60)
                    except TimeoutError:
                        raise Exception(f"Batch {b_id} timed out after {timeout_mins} minutes.")

                    if isinstance(res, Exception):
                        raise res
                    
                    # Carry over state: subsequent batches must use 'append'
                    _, is_first, _ = res
                    
                    _feature_service_log(
                        b_id, target_svc_name, target_server_url, batch_col,
                        item_id, write_mode, user, "Success", "Batch Complete",
                        log_table, attempt
                    )
                    
            success = True
            break
            
        except Exception as e:
            logger.error(f"Batch processing attempt {attempt} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
            attempt += 1

    return success