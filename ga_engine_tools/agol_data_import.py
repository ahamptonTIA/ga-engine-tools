# Standard Library Imports
import os
import re
import time
import logging
from datetime import datetime, timedelta
import html2text
import uuid
import hashlib

from .utils import *

# Third-Party Imports
import geoanalytics
from geoanalytics.sql import functions as ST  # Alias for GeoAnalytics SQL functions
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis.features import FeatureLayerCollection as FLC
import pandas as pd

# Import spark from databricks.sdk.runtime for Databricks notebooks
from databricks.sdk.runtime import spark

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ------------------------------------------------------------------------------------------

_logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------

def _upsert_download_log_entry_(entry_df, log_table):
    """
    Upserts log entries into the specified log table using 'layer_attempt_id' as the primary key.

    Parameters
    ----------
    entry_df : pyspark.sql.dataframe.DataFrame
        DataFrame containing the log entries to be upserted.
    log_table : str
        Name of the log table where entries will be upserted.

    Returns
    -------
    None
    """
    if log_table:
        entry_df.createOrReplaceTempView("log_updates_temp")
        # Cast all columns in entry_df to string for consistency with existing log_table
        cols_to_cast = [
            col for col in entry_df.columns if entry_df.schema[col].dataType != 'string'
        ]
        if cols_to_cast:
            entry_df = entry_df.select(
                [F.col(c).cast("string") if c in cols_to_cast else F.col(c) for c in entry_df.columns]
            )

        # Check if log_table exists and create if not
        if not spark._jsparkSession.catalog().tableExists(log_table):
            _logger.info(f"Creating log table: {log_table}")
            entry_df.write.mode('overwrite').format('delta').saveAsTable(log_table)
        else:
            # Use layer_attempt_id as the key for upserting individual layer log entries
            spark.sql(f"""
                MERGE INTO {log_table} AS target
                USING log_updates_temp AS source
                ON target.layer_attempt_id = source.layer_attempt_id 
                WHEN MATCHED THEN
                    UPDATE SET
                        target.status = source.status,
                        target.error_message = source.error_message,
                        target.timestamp = source.timestamp,
                        target.target_path = source.target_path,
                        target.layer_url = source.layer_url,
                        target.layer_name = source.layer_name,
                        target.load_id = source.load_id,
                        target.item_id = source.item_id,
                        target.sublayer_id = source.sublayer_id
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
        _logger.debug(f"Log table {log_table} updated.")

# ------------------------------------------------------------------------------------------

def ingest_agol_items_to_unity_catalog(
    item_list,
    target_schema,
    agol_inst: str = None,
    user: str = None,
    pswd: str = None,
    output_srid=4326,
    log_table=None,
    output_prefix=None,
    output_postfix=None,
):
    """
    Ingests ArcGIS Online feature layers into Unity Catalog tables.

    Parameters
    ----------
    item_list : list of str
        List of ArcGIS Online item IDs representing feature layers to ingest.
    target_schema : str
        Target Unity Catalog schema to save the tables.
    agol_inst : str, optional
        The URL of the ArcGIS Online instance. This is required for authenticating with ArcGIS.
    user : str, optional
        The username for ArcGIS authentication. This is required.
    pswd : str, optional
        The password for ArcGIS authentication. This is required.
    output_srid : int, optional
        Output spatial reference ID for reprojection (default is 4326).
    log_table : str, optional
        Table to log metadata of downloaded features (default is None).
    output_prefix : str, optional
        Prefix to add to the output table name (default is None).
    output_postfix : str, optional
        Postfix to add to the output table name (default is None).

    Returns
    -------
    list
        List of tables written to Unity Catalog.
    """
    out_tables = []
    items_properties = []

    gis = initialize_gis_connection(agol_inst, user, pswd)
    # Generate a unique load ID for this ingestion attempt
    load_id = str(uuid.uuid4())
    _logger.info(f"Starting ingestion with load_id: {load_id}")

    for item_id in item_list:
        item = gis.content.get(item_id)
        _logger.info(f"Processing item: {item.title} (ID: {item_id})")

        dfs = {}
        # Ensure item.layers exists and is iterable for feature service items
        if hasattr(item, 'layers') and item.layers:
            for layer in item.layers:
                layer_name_for_log = layer.properties.get('name', layer.url.split('/')[-1])
                sublayer_id_for_log = str(layer.properties.get('id', 'N/A'))

                # Generate cleaned table name from layer name
                table_name_parts = [
                    p for p in [output_prefix, layer_name_for_log, output_postfix] if p
                ]
                out_table_name = "_".join(table_name_parts)
                out_table_name = re.sub(r'[^a-zA-Z0-9_]+', '_', out_table_name).strip('_').lower()[:255]
                target_path_for_log = f"{target_schema}.{out_table_name}"

                # Generate a unique ID for this layer's ingestion attempt
                initial_layer_timestamp_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                hash_input = f"{target_path_for_log}-{initial_layer_timestamp_str}"
                layer_attempt_id = hashlib.md5(hash_input.encode('utf-8')).hexdigest()

                # Initial log entry (Pending status)
                initial_log_entry = {
                    "load_id": load_id,
                    "layer_attempt_id": layer_attempt_id,
                    "item_id": str(item_id),
                    "sublayer_id": sublayer_id_for_log,
                    "layer_url": str(layer.url),
                    "layer_name": str(layer_name_for_log),
                    "target_path": target_path_for_log,
                    "timestamp": initial_layer_timestamp_str,
                    "status": "Pending",
                    "error_message": None
                }

                _upsert_download_log_entry_(
                    spark.createDataFrame(pd.DataFrame([initial_log_entry])), log_table
                )
                _logger.info(
                    f"Logged 'Pending' status for layer: {layer_name_for_log} "
                    f"(ID: {sublayer_id_for_log}), Layer Attempt ID: {layer_attempt_id}"
                )

                status = 'Failed'
                error_message = None

                try:
                    # Read feature service layer into Spark DataFrame
                    print(f'Reading {layer.url}')
                    _df = spark.read.format("feature-service").option("gis", "GIS").load(layer.url)
                    # Reproject geometry columns to desired spatial reference
                    _df = reproject_df(_df, out_cs=output_srid)

                    # Store both the DataFrame and the layer's properties, including layer_attempt_id
                    layer.properties['out_table_name'] = out_table_name
                    layer.properties['layer_attempt_id'] = layer_attempt_id
                    dfs[layer.url] = {'df': _df, 'props': layer.properties}

                    status = 'Read & Reprojected'
                    _logger.info(
                        f"Layer {layer_name_for_log} (ID: {sublayer_id_for_log}) read and reprojected. "
                        f"Layer Attempt ID: {layer_attempt_id}"
                    )

                except Exception as e:
                    status = 'Failed'
                    error_message = str(e)
                    _logger.error(
                        f"Failed to read/reproject layer {layer_name_for_log} "
                        f"(ID: {sublayer_id_for_log}), Layer Attempt ID: {layer_attempt_id}: {e}"
                    )

                # Update log entry after read/reproject
                current_log_entry = {
                    "load_id": load_id,
                    "layer_attempt_id": layer_attempt_id,
                    "item_id": str(item_id),
                    "sublayer_id": sublayer_id_for_log,
                    "layer_url": str(layer.url),
                    "layer_name": str(layer_name_for_log),
                    "target_path": target_path_for_log,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                    "status": status,
                    "error_message": error_message
                }
                _upsert_download_log_entry_(
                    spark.createDataFrame(pd.DataFrame([current_log_entry])), log_table
                )

            # Store DataFrames and clean description text
            item.__dict__['dfs'] = dfs
            if 'description' in item.__dict__ and item.__dict__['description']:
                item.__dict__['description'] = html2text.html2text(item.__dict__['description'])
            else:
                item.__dict__['description'] = ""

            # Format creation and modification timestamps
            item.__dict__['created'] = datetime.fromtimestamp(item['created'] / 1000).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            item.__dict__['modified'] = datetime.fromtimestamp(item['modified'] / 1000).strftime(
                '%Y-%m-%d %H:%M:%S'
            )

            items_properties.append(item.__dict__)
        else:
            print(
                f"Item '{item.title}' (ID: {item_id}) is of type '{item.type}' and does not appear to have "
                f"layers directly accessible via .layers property or it has no layers."
            )
            _logger.warning(
                f"Item '{item.title}' (ID: {item_id}) has no accessible layers."
            )

    # Process and write DataFrames to Unity Catalog
    for item in items_properties:
        for lyr_url, data in item['dfs'].items():
            _df = data['df']
            lyr_props = data['props']

            layer_name = lyr_props.get('name')
            out_table_name = lyr_props.get('out_table_name')
            table_path = f"{target_schema}.{out_table_name}"
            out_tables.append(table_path)

            sublayer_id_for_log = str(lyr_props.get('id', 'N/A'))

            # Retrieve the layer_attempt_id directly from lyr_props
            layer_attempt_id = lyr_props.get('layer_attempt_id')
            if not layer_attempt_id:
                _logger.error(
                    f"Critical error: layer_attempt_id missing for {layer_name}. "
                    f"Cannot update log entry reliably."
                )
                continue

            # Extract spatial reference info from DataFrame
            sr_info = get_spatial_reference_info(_df)

            # Handle description merging
            lyr_desc_raw = lyr_props.get('description')
            final_desc = html2text.html2text(lyr_desc_raw) if lyr_desc_raw else ""

            if item['description'] and item['description'].strip():
                if final_desc:
                    final_desc = (
                        f"{final_desc}\n\n--- Parent Item Description ---\n{item['description']}"
                    )
                else:
                    final_desc = item['description']

            # Identify if it's a sublayer and add to description
            is_sublayer_text = ""
            if item['type'] == "Feature Service" and lyr_props.get('id') is not None:
                is_sublayer_text = (
                    f"**This is a sublayer of a Feature Service titled '{item['title']}' "
                    f"(ArcGIS Online ID: {item['id']})**\n\n"
                )

            # Compose table description with metadata and spatial reference details
            desc_text = (
                f"{is_sublayer_text}"
                f"# Table Name : {out_table_name}\n"
                f"- ArcGIS Online ID: \n    - `{item['id']}`\n"
                f"- Service Layer ID: \n    - `{lyr_props.get('id')}`\n"
                f"- ArcGIS Online Creation Date: \n    - `{item['created']}`\n"
                f"- ArcGIS Online Last Modified Date: \n    - `{item['modified']}`\n"
                f"- Data Migration/Pull Date: \n    - `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
                f"- Description: \n    - {final_desc}\n"
                f"- Spatial Reference Properties:\n"
            )
            for k, v in sr_info.items():
                desc_text += f"    - {k} : {v}\n"

            # Handle tags merging
            combined_tags = list(item['tags'])

            layer_tags = lyr_props.get('tags')
            if layer_tags and isinstance(layer_tags, list):
                combined_tags.extend(layer_tags)
            elif layer_tags and isinstance(layer_tags, str):
                combined_tags.extend([t.strip() for t in layer_tags.split(',') if t.strip()])

            clean_additional_tags = [
                re.sub(r'[^a-zA-Z0-9_]', '_', tag).strip('_')
                for tag in [item['id'], item['title'], layer_name] if tag
            ]
            combined_tags.extend(clean_additional_tags)

            # Ensure tags are unique and clean for Databricks criteria
            disallowed_chars_pattern = r'[.,\-=/:]'
            max_tag_key_length = 255
            max_tags_count = 50

            final_tags = []
            seen_tags = set()
            for tag in combined_tags:
                cleaned_tag = re.sub(disallowed_chars_pattern, '_', tag.strip())[:max_tag_key_length]
                if cleaned_tag and cleaned_tag not in seen_tags:
                    final_tags.append(cleaned_tag)
                    seen_tags.add(cleaned_tag)

            final_tags = final_tags[:max_tags_count]

            geom_col = sr_info['Geometry Column']

            print(desc_text)

            # Convert geometry column to WKT and rename column
            _df = _df.withColumn(geom_col, ST.as_text(_df[geom_col]))
            _df = _df.withColumnRenamed(geom_col, "geometry_wkt")

            log_status = "Complete"
            log_error = None
            warn_msgs = []

            try:
                # Write DataFrame to Unity Catalog table with schema merge
                _logger.info(f"Writing table: {table_path}")
                _df.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable(table_path)
            except Exception as e:
                log_status = "Failed"
                log_error = str(e)
                w_msg = f"Failed to write table {table_path}: {e}"
                print(w_msg)
                _logger.error(w_msg)

            # Set table properties for description and tags
            try:
                spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('comment' = {repr(desc_text)})")
                _logger.info(f"Set comment for table: {table_path}")
            except Exception as e:
                warn_msgs.append(f"Failed to set table properties (comment): {e}")
                _logger.warning(f"Failed to set table properties (comment) for {table_path}: {e}")
                if log_status != "Failed":
                    log_status = "Completed with Warnings"

            try:
                tag_sql_list = [f"'{tag}'" for tag in final_tags]
                if tag_sql_list:
                    spark.sql(f"ALTER TABLE {table_path} SET TAGS ({', '.join(tag_sql_list)})")
                    _logger.info(f"Set tags for table: {table_path}")
            except Exception as e:
                warn_msgs.append(f"Failed to set tags: {e}")
                _logger.warning(f"Failed to set tags for {table_path}: {e}")
                if log_status != "Failed":
                    log_status = "Completed with Warnings"

            # Final log entry update (Complete/Failed/Warnings)
            final_error_message = None
            if log_error or warn_msgs:
                msgs = []
                if log_error:
                    msgs.append(log_error)
                if warn_msgs:
                    msgs.extend(warn_msgs)
                final_error_message = "\n".join(msgs)

            final_log_entry = {
                "load_id": load_id,
                "layer_attempt_id": layer_attempt_id,
                "item_id": str(item['id']),
                "sublayer_id": sublayer_id_for_log,
                "layer_url": str(lyr_url),
                "layer_name": str(layer_name),
                "target_path": table_path,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                "status": log_status,
                "error_message": final_error_message
            }
            _upsert_download_log_entry_(
                spark.createDataFrame(pd.DataFrame([final_log_entry])), log_table
            )
            _logger.info(
                f"Logged '{log_status}' status for table: {table_path}, Layer Attempt ID: {layer_attempt_id}"
            )

    _logger.info(f"Ingestion process completed for load_id: {load_id}")
    return out_tables

# ------------------------------------------------------------------------------------------