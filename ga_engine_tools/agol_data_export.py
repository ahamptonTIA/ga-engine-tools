import os
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from .utils import *

import geoanalytics
from geoanalytics.sql import functions as ST

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

    Connects to an ArcGIS Feature Service, retrieves its schema, and converts it
    into a PySpark-compatible DataFrame with the appropriate schema.

    Parameters
    ----------
    input_df : pyspark.sql.DataFrame
        The input Spark DataFrame to be cast.
    layer_url : str
        The URL of the ArcGIS Feature layer.
    gis_object : arcgis.gis.GIS
        The GIS object for connecting to the ArcGIS service.

    Returns
    -------
    pyspark.sql.DataFrame
        A Spark DataFrame with the schema of the feature layer.

    Raises
    ------
    Exception
        If any error occurs during schema retrieval or casting.
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
        _logger.error(f"An error occurred: {e}")
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

    Parameters
    ----------
    batch_id_value : str
        The batch ID value.
    target_svc_name : str
        The target service name.
    target_server_url : str
        The target server URL.
    batch_by_column : str
        The column used for batching.
    layer_id : int
        The layer ID.
    write_mode : str
        The write mode (e.g., 'overwrite', 'append').
    user : str
        The user performing the operation.
    status : str
        The status of the operation ('Timeout', 'Failed', 'Success').
    message : str
        The log message.
    logTable : str
        The log table name.
    attempt_count : int
        The attempt count.

    Raises
    ------
    ValueError
        If status is not one of 'Timeout', 'Failed', or 'Success'.
    """
    if status not in ["Failed", "Success", "Timeout"]:
        raise ValueError("Status must be 'Timeout', 'Failed' or 'Success'.")

    log_message = message if message else (
        "Complete" if status == "Success" else "Unknown error"
    )

    if logTable:
        log_entry = [(
            batch_id_value, target_svc_name, target_server_url,
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
                existing_entry = (
                    spark.table(logTable)
                    .filter(F.col("batch") == batch_id_value)
                    .count()
                )
                if existing_entry > 0:
                    log_df.createOrReplaceTempView("log_temp_view")
                    spark.sql(f"""
                        MERGE INTO {logTable} AS target
                        USING log_temp_view AS source
                        ON target.batch = '{batch_id_value}'
                        WHEN MATCHED THEN
                            UPDATE SET
                                target.load_timestamp = '{datetime.now()}',
                                target.status = '{status}',
                                target.message = '{log_message.replace("'", "''")}',
                                target.retry_attempts = {attempt_count},
                                target.target_svc_name = '{target_svc_name}',
                                target.target_server_url = '{target_server_url}',
                                target.batch_by_column = '{batch_by_column}',
                                target.layer_id = {layer_id},
                                target.write_mode = '{write_mode}',
                                target.user = '{user}'
                        WHEN NOT MATCHED THEN
                            INSERT (
                                batch, target_svc_name, target_server_url,
                                batch_by_column, layer_id, write_mode, user,
                                load_timestamp, status, message, retry_attempts
                            )
                            VALUES (
                                '{batch_id_value}', '{target_svc_name}',
                                '{target_server_url}', '{batch_by_column}',
                                {layer_id}, '{write_mode}', '{user}',
                                '{datetime.now()}', '{status}',
                                '{log_message.replace("'", "''")}',
                                {attempt_count}
                            )
                    """)
                else:
                    log_df.write.mode("append").saveAsTable(logTable)
            else:
                log_df.write.mode("append").saveAsTable(logTable)
        except Exception as e:
            _logger.error(
                f"Error saving log entry to table '{logTable}': {e}"
            )
    print(
        f"Batch '{batch_id_value}' {status.lower()}: {log_message} "
        f"(Attempt {attempt_count})"
    )

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
    Checks if a Feature Service exists on a given GIS by its name.
    Optionally creates a new empty Feature Service if it doesn't exist.

    Parameters
    ----------
    gis_object : arcgis.gis.GIS
        An already established GIS object.
    service_name : str
        The exact name (title) of the Feature Service to check for/create.
    folder : str, optional
        The folder where the feature service is located.
    create_if_not_exists : bool, optional
        If True, the service will be created if it doesn't exist. Defaults to False.
    owner_username : str, optional
        The username of the owner to restrict the search to. Defaults to None.
    summary : str, optional
        A short summary for the new Feature Service if created. Defaults to None.
    tags : list, optional
        A list of tags for the new Feature Service if created. Defaults to None.

    Returns
    -------
    tuple
        (service_found, service_url, service_id, layer_info_list)
        service_found : bool
            True if the Feature Service was found or successfully created, False otherwise.
        service_url : str or None
            The URL of the Feature Service if found or created, None otherwise.
        service_id : str or None
            The Item ID (AGOL Item ID) of the Feature Service if found or created, None otherwise.
        layer_info_list : list of dict
            Each dictionary represents a layer or table within the Feature Service and contains
            'name' and 'id' (service layer ID). Returns an empty list if no layers/tables exist
            or if the service is new.

    Raises
    ------
    Exception
        If any error occurs during the GIS operation (e.g., connection issues, permission errors).
    """
    if not isinstance(gis_object, GIS):
        _logger.error("Invalid GIS object provided.")
        return False, None, None, []

    owner_str = '\u200B'.join(owner_username) if owner_username else None
    search_scope_msg = (
        f"content owned by '{owner_str}'"
        if owner_username else "all accessible content in the organization"
    )
    _logger.info(
        f"Checking feature service: '{service_name}' within {search_scope_msg}..."
    )

    try:
        search_query = f"title:\"{service_name}\" AND type:\"Feature Service\""
        if owner_username:
            search_query += f" AND owner:\"{owner_username}\""
            _logger.info(f"Restricting search to owner: '{owner_str}'")

        feature_services = gis_object.content.search(
            query=search_query, item_type="Feature Service"
        )

        found_service = None
        for service_item in feature_services:
            if service_item.title == service_name:
                found_service = service_item
                break

        if found_service:
            _logger.info(
                f"Feature Service '{service_name}' exists... Retrieving info on layer(s)."
            )
            layer_info_list = []
            try:
                flc = FLC.fromitem(found_service)
                for layer in flc.layers:
                    layer_info_list.append({
                        'name': layer.properties.get('name'),
                        'id': layer.properties.get('id')
                    })
                for table in flc.tables:
                    layer_info_list.append({
                        'name': table.properties.get('name'),
                        'id': table.properties.get('id')
                    })
                _logger.info(
                    f"Found {len(layer_info_list)} layers/tables in '{service_name}'."
                )
            except Exception as layer_e:
                _logger.warning(
                    f"Could not retrieve layer info for '{service_name}': {layer_e}"
                )
                layer_info_list = []

            return (
                True, found_service.url, found_service.id, layer_info_list
            )
        else:
            _logger.info(f"Feature Service '{service_name}' not found.")
            if create_if_not_exists:
                _logger.info(
                    f"Attempting to create Feature Service: '{service_name}'..."
                )
                created_service = gis_object.content.create_service(
                    name=service_name,
                    capabilities="Query,Editing,Create",
                    service_type="feature",
                    folder=folder,
                    description=summary if summary else f"Feature Service for {service_name}",
                    service_description=summary if summary else f"Feature Service for {service_name}",
                    tags=tags if tags else ["automated_creation", "python_api"]
                )

                time.sleep(10)
                if created_service:
                    _logger.info(
                        f"Successfully created Feature Service: '{created_service.title}' (ID: {created_service.id})."
                    )
                    _logger.info("Service is ready for sublayers to be added.")
                    return (
                        True, created_service.url, created_service.id, []
                    )
                else:
                    _logger.error(
                        f"Failed to create Feature Service '{service_name}'."
                    )
                    return False, None, None, []
            else:
                _logger.info(
                    "Creation was not requested (`create_if_not_exists` = False)."
                )
                return False, None, None, []

    except Exception as e:
        _logger.error(
            f"An error occurred during service check/creation: {e}"
        )
        return False, None, None, []

# ------------------------------------------------------------------------------------------

def check_feature_service_existence_and_compare(
    input_df: DataFrame,
    service_name: str,
    layer_name: str,
    gis: GIS,
    batch_by_column_name: str,
    create_if_not_exists: bool = True,
    folder: str = None,
    owner_username: str = None,
    summary: str = None,
    tags: list = None
):
    """
    Check if the feature service exists and compare unique IDs in the input DataFrame
    with the existing feature service.

    Parameters
    ----------
    input_df : pyspark.sql.DataFrame
        The input Spark DataFrame.
    service_name : str
        The name of the target ArcGIS Feature Service.
    layer_name : str
        The name of the target ArcGIS Feature layer.
    gis : arcgis.gis.GIS
        The GIS object for ArcGIS authentication.
    batch_by_column_name : str
        The name of the column in `input_df` that contains unique identifiers for comparison.
    create_if_not_exists : bool, optional
        If True, create the service if it does not exist. Default is True.
    folder : str, optional
        The folder where the feature service is located.
    owner_username : str, optional
        The username of the owner to restrict the search to.
    summary : str, optional
        The summary of the feature service.
    tags : list, optional
        The tags associated with the feature service.

    Returns
    -------
    tuple
        (svc_exists, service_url, lyr_exists, lyr_id, ext_unique_ids, new_unique_ids, df_match)
        svc_exists : bool
            True if the service exists, False otherwise.
        service_url : str
            The URL of the service.
        lyr_exists : bool
            True if the layer exists, False otherwise.
        lyr_id : int
            The ID of the layer.
        ext_unique_ids : list
            List of unique IDs from the existing feature service.
        new_unique_ids : list
            List of unique IDs from the input DataFrame.
        df_match : bool
            True if the DataFrame matches the feature service, False otherwise.
    """
    new_unique_ids = (
        input_df.select(batch_by_column_name)
        .distinct().rdd.map(lambda row: row[0]).collect()
    )

    svc_exists = False
    ext_unique_ids = []
    try:
        svc_exists, service_url, service_id, layer_list = check_create_feature_service(
            gis,
            service_name=service_name,
            create_if_not_exists=create_if_not_exists,
            owner_username=owner_username,
            folder=folder,
            summary=summary,
            tags=tags
        )

        lyr_id = next(
            (layer['id'] for layer in layer_list if layer['name'] == layer_name),
            None
        )

        if lyr_id is not None:
            layer_url = f'{service_url}/{lyr_id}'
            feature_layer = FeatureLayer(layer_url, gis=gis)
            if feature_layer.properties:
                lyr_exists = True
                print(f"\t\t-Feature layer '{layer_name}' exists and is accessible.")
                input_df = set_pyspark_schema_from_feature_service(
                    input_df, layer_url, gis
                )
                ext_feats_df = (
                    spark.read.format("feature-service")
                    .option("gis", "GIS")
                    .load(layer_url)
                )
                ext_feats_cnt = ext_feats_df.count()
                print(
                    f"\t\t-Successfully read {ext_feats_cnt} records from existing service for comparison."
                )
                ext_unique_ids = (
                    ext_feats_df.select(batch_by_column_name)
                    .distinct().rdd.map(lambda row: row[0]).collect()
                )
        else:
            lyr_exists = False
            lyr_id = 0
            print(
                f"\t\t-Feature layer '{layer_name}' does not exists or is unaccessible."
            )
    except Exception as e:
        print(f"\t\t-Feature layer '{layer_name}' not found...")
        print(f"\t\t-Will create a new feature layer: {layer_name}")
        svc_exists = False
        lyr_exists = False
        ext_unique_ids = []

    df_match = (
        all(uid in ext_unique_ids for uid in new_unique_ids) and
        all(uid in new_unique_ids for uid in ext_unique_ids)
    )

    return (
        svc_exists, service_url, lyr_exists, lyr_id,
        ext_unique_ids, new_unique_ids, df_match
    )

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
    Write data to the feature service in chunks to prevent memory errors.

    Parameters
    ----------
    df_to_write : pyspark.sql.DataFrame
        The Spark DataFrame containing the data to write.
    is_first_batch : bool
        Flag indicating if this is the first batch being written.
    initial_write_mode : str
        The mode for writing data to the feature service (e.g., 'overwrite', 'append').
    parallelism : int
        The level of parallelism to use for writing data.
    service_url : str
        The URL of the feature service.
    layer_name : str
        The name of the target ArcGIS Feature layer.
    retry_delay_seconds : int, optional
        The delay in seconds between each retry attempt (default is 30).
    attempt_count : int, optional
        The current attempt count for writing data (default is 1).
    max_chunk_size : int, optional
        The maximum number of rows per chunk to write (default is 500000).

    Returns
    -------
    tuple
        (lyr_exists, is_first_batch, effective_write_mode)
        lyr_exists : bool
            True if the layer exists, False otherwise.
        is_first_batch : bool
            False after the first chunk is written.
        effective_write_mode : str
            The effective write mode used ('append' after the first chunk).

    Raises
    ------
    Exception
        If any error occurs during the write operation.
    """
    _logger.info(f"Attempt {attempt_count}")
    if attempt_count > 1:
        _logger.info("Will reset AGOL connections for each chunk.")
    _logger.info(f"Chunking load attempts by {max_chunk_size}.")

    load_time_start = datetime.now()
    total_rows = df_to_write.count()
    num_chunks = (total_rows // max_chunk_size) + (
        1 if total_rows % max_chunk_size != 0 else 0
    )

    temp_col = '__temp_chunk_col__'
    window_spec = Window.orderBy(F.monotonically_increasing_id())

    df_to_write = df_to_write.withColumn(
        temp_col,
        F.floor(
            (F.row_number().over(window_spec) - F.lit(1)) / F.lit(max_chunk_size)
        ).cast("int") + F.lit(1)
    )

    max_chunk_num = df_to_write.select(F.col(temp_col)).distinct().count()

    for chunk_num in range(1, max_chunk_num + 1):
        if attempt_count > 1:
            gis = _reset_connection_(
                retry_delay_seconds, agol_inst, user, pswd, False
            )
        chunk_df = df_to_write.filter(F.col(temp_col) == chunk_num).drop(temp_col)

        effective_write_mode = (
            initial_write_mode if is_first_batch and chunk_num == 1 else "append"
        )

        _logger.info(
            f"Writing chunk {chunk_num}/{max_chunk_num} in '{effective_write_mode}' "
            f"mode using parallelism of {parallelism}. Chunk Size: {max_chunk_size}"
        )

        try:
            chunk_df.write.format("feature-service") \
                .option("gis", "GIS") \
                .option("maxParallelism", parallelism) \
                .option("serviceUrl", service_url) \
                .option("layerName", layer_name) \
                .mode(effective_write_mode) \
                .save()
            effective_write_mode = 'append'
            is_first_batch = False

        except Exception as e:
            return e

    return True, False, effective_write_mode

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
    max_chunk_size: int = None,
    retry_delay_seconds: int = 30,
    maxParallelism: int = 12,
    logTable: str = None,
    timeout_minutes: int = 15,
    folder: str = None,
    service_summary: str = None,
    service_tags: list = None
) -> bool:
    """
    Updates an ArcGIS Feature Service layer with data from a Spark DataFrame.

    This function includes retry mechanisms for the entire update process.
    It will repeatedly check if the data in the input DataFrame matches the
    data in the feature service. If not, it attempts to write the data,
    retrying failed individual batches (identified by batch_by_column_name) in
    subsequent full attempts. The process continues until a match is confirmed
    or max_total_retries is reached.

    Parameters
    ----------
    input_df : pyspark.sql.DataFrame
        The Spark DataFrame containing the data to write. It is expected to have a column named by `batch_by_column_name`.
    target_svc_name : str
        The name of the target ArcGIS Feature Service (e.g., 'bead_post_challenge_locations').
    target_server_url : str
        The base URL of the ArcGIS Feature Server (e.g., "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/arcgis/rest/
import os
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, TimeoutError

from .utils import *

import geoanalytics
from geoanalytics.sql import functions as ST

$0services").
    batch_by_column_name : str
        The name of the column in `input_df` that contains unique identifiers for comparison and batching.
    layer_name : str, optional
        The name of the target ArcGIS Feature layer.
    write_mode : str, optional
        The mode for writing data to the feature service. Default is "overwrite".
    agol_inst : str, optional
        The URL of the ArcGIS Online instance. This is required for authenticating with ArcGIS.
    user : str, optional
        The username for ArcGIS authentication. This is required.
    pswd : str, optional
        The password for ArcGIS authentication. This is required.
    max_total_retries : int, optional
        The maximum number of times the entire update process will be retried if the data does not match.
    max_chunk_size : int, optional
        The maximum number of records to include in each batch write attempt.
    retry_delay_seconds : int, optional
        The delay in seconds between each full retry attempt.
    maxParallelism : int, optional
        Maximum number of parallel tasks for writing data to the feature service (default is 12).
    logTable : str, optional
        The fully qualified table name (e.g., "database.schema.table") to log the status of each batch write attempt.
    timeout_minutes : int, optional
        The timeout value in minutes for each batch load (default is 15).
    folder : str, optional
        The folder where the feature service is located.
    service_summary : str, optional
        The summary of the feature service.
    service_tags : list, optional
        The tags associated with the feature service.

    Returns
    -------
    bool
        True if the feature service eventually matched the input DataFrame,
        False otherwise (e.g., if max_total_retries was reached without a match).
    """
    global gis
    if 'gis' not in globals() or not isinstance(gis, GIS):
        gis = initialize_gis_connection(agol_inst, user, pswd)

    initial_write_mode = write_mode[:]
    parallelism = maxParallelism
    attempt_count = 1
    batch_match = False

    failed_batches_current_attempt = set()

    while not batch_match and attempt_count <= max_total_retries:
        svc_exists, service_url, lyr_exists, layer_id, ext_unique_ids, new_unique_ids, batch_match = \
            check_feature_service_existence_and_compare(
                input_df=input_df,
                service_name=target_svc_name,
                layer_name=layer_name,
                gis=gis,
                batch_by_column_name=batch_by_column_name,
                owner_username=user,
                folder=folder,
                summary=service_summary,
                tags=service_tags
            )

        layer_url = f'{service_url}/{layer_id}'

        if batch_match and write_mode != "overwrite":
            print("\t-Batch Id's match, update complete.\n")
            return True
        else:
            print(f"\n\t--- Starting update attempt {attempt_count}/{max_total_retries} ---")

        batch_id_to_process = []
        if write_mode == "append":
            batch_id_to_process = list(set(new_unique_ids) - set(ext_unique_ids))
        elif write_mode == "overwrite":
            batch_id_to_process = list(set(new_unique_ids))

        if failed_batches_current_attempt:
            print(f"\tRetrying {len(failed_batches_current_attempt)} batches that failed in the previous attempt.")
            batch_id_to_process.extend(list(failed_batches_current_attempt))
            failed_batches_current_attempt.clear()

        if not batch_id_to_process:
            print(
                f"\tNo new batch IDs to add or previously failed batch IDs to retry in this attempt. "
                f"Current service has {len(ext_unique_ids)} Batch IDs, input DataFrame has {len(new_unique_ids)} Batch IDs."
            )
        else:
            print(f"\t-Found {len(batch_id_to_process)} batches to process (new or retrying) in this attempt.")

        with ThreadPoolExecutor(max_workers=1) as executor:
            for idx, batch_id in enumerate(sorted(batch_id_to_process), start=1):
                first_batch = True if idx == 1 else False

                if lyr_exists and idx >= 2:
                    input_df = set_pyspark_schema_from_feature_service(
                        input_df, layer_url, gis
                    )

                print(
                    f"\t-Attempting to write data for batch ID ({batch_by_column_name}): "
                    f"{batch_id} ({idx}/{len(batch_id_to_process)})"
                )

                try:
                    subset_df = input_df.filter(F.col(batch_by_column_name) == batch_id)
                    future = executor.submit(
                        write_df_in_chunks,
                        subset_df,
                        first_batch,
                        write_mode,
                        parallelism,
                        service_url,
                        layer_name,
                        retry_delay_seconds,
                        attempt_count,
                        max_chunk_size
                    )

                    load_time_start = datetime.now()
                    load_result = future.result(timeout=timeout_minutes * 60)
                    load_time_end = datetime.now()
                    time_to_load = load_time_end - load_time_start

                    if isinstance(load_result, Exception):
                        raise load_result
                    lyr_exists, first_batch, write_mode = load_result

                    status = "Success"
                    message = f"Complete; Batch: {batch_id}, completed in: {time_to_load}"
                    parallelism = min(parallelism + 1, maxParallelism)

                except TimeoutError:
                    print(
                        f"\t-Timeout: Data for batch ID {batch_id} took longer than {timeout_minutes} minutes. "
                        "Resetting connection and will retry this batch in the next overall attempt."
                    )
                    status = "Failed"
                    message = f"Timed out; Batch: {batch_id}, timed out after {timeout_minutes} minutes"
                    failed_batches_current_attempt.add(batch_id)
                    gis = _reset_connection_(retry_delay_seconds, agol_inst, user, pswd)
                    parallelism = int(max(1, parallelism // 2))
                    max_chunk_size = int(max_chunk_size // 2)
                    time.sleep(retry_delay_seconds)

                except Exception as e:
                    print(
                        f"\t-Error: Data for batch ID {batch_id} failed to load! Error: {e}"
                    )
                    failed_batches_current_attempt.add(batch_id)
                    status = "Failed"
                    message = f"Error; Batch: {batch_id}, failed with error: {e}"
                    gis = _reset_connection_(retry_delay_seconds, agol_inst, user, pswd)
                    parallelism = int(max(1, parallelism // 2))
                    max_chunk_size = int(max_chunk_size // 2)

                finally:
                    _feature_service_log_(
                        batch_id,
                        target_svc_name,
                        target_server_url,
                        batch_by_column_name,
                        layer_id,
                        write_mode,
                        user, status, message,
                        logTable, attempt_count
                    )

                    attempt_count += 1

        if not batch_match and attempt_count > max_total_retries:
            print(f"\n\t-Maximum retry attempts ({max_total_retries}) reached!")
            print("\t\t-Data still does not match, aborting update!")
            return False

    return True

# ------------------------------------------------------------------------------------------