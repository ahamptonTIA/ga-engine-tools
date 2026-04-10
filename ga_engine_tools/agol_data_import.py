"""
Utilities for ingesting ArcGIS Online feature layers into Databricks Unity Catalog.

This module automates the extraction of spatial data and its associated metadata,
performing reprojection and standardizing storage in Delta tables.
"""

import re
import uuid
import hashlib
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

# Third-party GIS and Data Analysis
import pandas as pd
import html2text
import geoanalytics
# from geoanalytics.sql import functions as GAE 
from arcgis.gis import GIS

# Databricks & Spark Native
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F, DataFrame

# Local Modules
from .utils import (
    initialize_gis_connection,
    reproject_df,
    get_spatial_reference_info
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Internal Helpers
# -----------------------------------------------------------------------------

def _upsert_download_log_entry(
    entry_df: DataFrame, 
    log_table: str
) -> None:
    """
    Upsert log entries into the specified Delta log table.

    Parameters
    ----------
    entry_df : pyspark.sql.DataFrame
        DataFrame containing a single log entry.
    log_table : str
        The full path (catalog.schema.table) of the target log table.
    """
    if not log_table:
        return

    # Ensure all columns are strings for unified log schema consistency
    cols_to_cast = [
        col for col in entry_df.columns 
        if str(entry_df.schema[col].dataType) != 'StringType'
    ]
    
    if cols_to_cast:
        entry_df = entry_df.select([
            F.col(c).cast("string") if c in cols_to_cast else F.col(c) 
            for c in entry_df.columns
        ])

    entry_df.createOrReplaceTempView("log_updates_temp")

    # Check if log_table exists using the public Catalog API
    if not spark.catalog.tableExists(log_table):
        logger.info(f"Initializing new download log table: {log_table}")
        entry_df.write \
            .mode('overwrite') \
            .format('delta') \
            .saveAsTable(log_table)
    else:
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
    logger.debug(f"Log entry upserted for {log_table}.")

# -----------------------------------------------------------------------------
# Core Ingestion Logic
# -----------------------------------------------------------------------------

def ingest_agol_items_to_unity_catalog(
    item_list: List[str],
    target_schema: str,
    agol_inst: Optional[str] = None,
    user: Optional[str] = None,
    pswd: Optional[str] = None,
    output_srid: int = 4326,
    log_table: Optional[str] = None,
    output_prefix: Optional[str] = None,
    output_postfix: Optional[str] = None,
) -> List[str]:
    """
    Ingest ArcGIS Online feature layers into Unity Catalog Delta tables.

    This function iterates through a list of AGOL Item IDs, extracts all 
    sublayers, reprojects the geometry, and writes them to Unity Catalog 
    while preserving descriptions and tags as table metadata.

    Parameters
    ----------
    item_list : List[str]
        List of ArcGIS Online Item IDs (GUID strings).
    target_schema : str
        The catalog and schema in Unity Catalog (e.g., 'main.default').
    agol_inst : str, optional
        ArcGIS Online instance URL.
    user : str, optional
        ArcGIS username.
    pswd : str, optional
        ArcGIS password.
    output_srid : int, default 4326
        The target spatial reference (WKID) for the output Delta tables.
    log_table : str, optional
        Full path to a Delta table for tracking ingestion status.
    output_prefix : str, optional
        String to prepend to the resulting table names.
    output_postfix : str, optional
        String to append to the resulting table names.

    Returns
    -------
    List[str]
        A list of the full Unity Catalog paths for all successfully created tables.
    """
    out_tables = []
    items_properties = []
    h2t = html2text.HTML2Text()
    h2t.ignore_links = False

    gis = initialize_gis_connection(agol_inst, user, pswd)
    if not gis:
        return []
    
    load_id = str(uuid.uuid4())
    logger.info(f"Starting ingestion process | load_id: {load_id}")

    for item_id in item_list:
        item = gis.content.get(item_id)
        if not item:
            logger.error(f"Could not retrieve item {item_id}. Skipping.")
            continue

        logger.info(f"Processing Item: {item.title}")
        dfs = {}

        if not hasattr(item, 'layers') or not item.layers:
            logger.warning(f"Item '{item.title}' has no accessible layers.")
            continue

        for layer in item.layers:
            layer_name = layer.properties.get('name', 'unnamed_layer')
            sublayer_id = str(layer.properties.get('id', '0'))

            # Sanitize table name for Unity Catalog (No special chars, lowercase)
            name_components = [p for p in [output_prefix, layer_name, output_postfix] if p]
            raw_name = "_".join(name_components)
            clean_name = re.sub(r'[^a-zA-Z0-9_]+', '_', raw_name).strip('_').lower()[:255]
            
            target_path = f"{target_schema}.{clean_name}"

            # Generate unique hash for this specific attempt
            now = datetime.now()
            attempt_hash = f"{target_path}-{now.isoformat()}"
            layer_attempt_id = hashlib.md5(attempt_hash.encode('utf-8')).hexdigest()

            # Log Pending Status
            log_data = {
                "load_id": load_id,
                "layer_attempt_id": layer_attempt_id,
                "item_id": str(item_id),
                "sublayer_id": sublayer_id,
                "layer_url": str(layer.url),
                "layer_name": str(layer_name),
                "target_path": target_path,
                "timestamp": now.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "status": "Pending",
                "error_message": None
            }
            _upsert_download_log_entry(spark.createDataFrame([log_data]), log_table)

            try:
                print(f"  > Reading Layer: {layer_name}")
                _df = spark.read.format("feature-service").option("gis", "GIS").load(layer.url)
                
                # Perform Reprojection
                _df = reproject_df(_df, out_cs=output_srid)

                layer.properties['out_table_name'] = clean_name
                layer.properties['layer_attempt_id'] = layer_attempt_id
                dfs[layer.url] = {'df': _df, 'props': layer.properties}

                log_data["status"] = "Data Read"
            except Exception as e:
                log_data["status"] = "Failed"
                log_data["error_message"] = str(e)
                logger.error(f"Read failed for {layer_name}: {e}")

            _upsert_download_log_entry(spark.createDataFrame([log_data]), log_table)

        # Process metadata for the item
        item_dict = item.__dict__.copy()
        item_dict['dfs'] = dfs
        
        # Clean HTML from descriptions
        raw_desc = item_dict.get('description', "")
        item_dict['description_cleaned'] = h2t.handle(raw_desc) if raw_desc else ""

        # Normalize timestamps
        for ts_key in ['created', 'modified']:
            if ts_key in item_dict:
                item_dict[ts_key] = datetime.fromtimestamp(item_dict[ts_key] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        
        items_properties.append(item_dict)

    # Write stage: Commit to Unity Catalog
    for item_meta in items_properties:
        for lyr_url, data in item_meta['dfs'].items():
            _df = data['df']
            lyr_props = data['props']
            table_path = f"{target_schema}.{lyr_props['out_table_name']}"
            
            sr_info = get_spatial_reference_info(_df)
            geom_col = sr_info['Geometry Column']

            # Standardize: Convert GAE Binary to WKT for storage in Delta
            _df = _df.withColumn(geom_col, GAE.as_text(_df[geom_col]))
            _df = _df.withColumnRenamed(geom_col, "geometry_wkt")

            # Build metadata comment for Unity Catalog
            lyr_desc_raw = lyr_props.get('description', "")
            lyr_desc = h2t.handle(lyr_desc_raw) if lyr_desc_raw else "No layer description provided."
            
            comment_body = (
                f"Source: ArcGIS Online (ID: {item_meta['id']})\n"
                f"Layer Name: {lyr_props.get('name')}\n"
                f"Ingested: {datetime.now().strftime('%Y-%m-%d')}\n\n"
                f"--- Description ---\n{lyr_desc}\n"
                f"--- Parent Item Description ---\n{item_meta['description_cleaned']}\n"
            )

            # Sanitize and prepare tags
            tags = list(item_meta.get('tags', []))
            if isinstance(lyr_props.get('tags'), list):
                tags.extend(lyr_props['tags'])
            
            unique_tags = {re.sub(r'[^a-zA-Z0-9]', '_', str(t)).lower()[:255] for t in tags if t}
            final_tags = sorted(list(unique_tags))[:50] # UC allows up to 50 tags

            try:
                logger.info(f"Writing to Unity Catalog: {table_path}")
                _df.write.mode('overwrite').option('mergeSchema', 'true').saveAsTable(table_path)
                
                # Apply metadata
                spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('comment' = {repr(comment_body)})")
                if final_tags:
                    tag_stmt = ", ".join([f"'{t}'" for t in final_tags])
                    spark.sql(f"ALTER TABLE {table_path} SET TAGS ({tag_stmt})")

                out_tables.append(table_path)
                status, error = "Complete", None
            except Exception as e:
                logger.error(f"Write failed for {table_path}: {e}")
                status, error = "Failed", str(e)

            # Final Log Update
            final_log = {
                "load_id": load_id,
                "layer_attempt_id": lyr_props['layer_attempt_id'],
                "item_id": str(item_meta['id']),
                "sublayer_id": str(lyr_props.get('id', '0')),
                "layer_url": str(lyr_url),
                "layer_name": str(lyr_props.get('name')),
                "target_path": table_path,
                "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
                "status": status,
                "error_message": error
            }
            _upsert_download_log_entry(spark.createDataFrame([final_log]), log_table)

    logger.info(f"Ingestion complete. {len(out_tables)} tables processed.")
    return out_tables