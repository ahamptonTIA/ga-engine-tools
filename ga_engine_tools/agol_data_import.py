import re
import uuid
import hashlib
import logging
from datetime import datetime
from typing import List, Optional

# Third-party GIS and Data Analysis
import pandas as pd
import html2text
import geoanalytics
# Aliased to GAE to facilitate the .as_text() conversion for WKT output
from geoanalytics.sql import functions as GAE 
from arcgis.gis import GIS

# Databricks & Spark Native
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F, DataFrame

# Local Modules (Standardized with utils.py)
from .utils import (
	initialize_gis_connection,
	reproject_df,
	get_spatial_reference_info
)

# -----------------------------------------------------------------------------

# Use __name__ so logs are categorized by this specific submodule
_logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------

def _upsert_download_log_entry_(
	entry_df: DataFrame, 
	log_table: str
) -> None:
	"""
	Upsert log entries into the specified log table.
	"""
	if not log_table:
		return

	entry_df.createOrReplaceTempView("log_updates_temp")
	
	# Cast all columns in entry_df to string for consistency with existing log
	cols_to_cast = [
		col for col in entry_df.columns 
		if entry_df.schema[col].dataType != 'string'
	]
	
	if cols_to_cast:
		entry_df = entry_df.select([
			F.col(c).cast("string") if c in cols_to_cast else F.col(c) 
			for c in entry_df.columns
		])

	# Check if log_table exists and create if not
	if not spark._jsparkSession.catalog().tableExists(log_table):
		_logger.info(f"Creating log table: {log_table}")
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
	_logger.debug(f"Log table {log_table} updated.")

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
	Ingest ArcGIS Online feature layers into Unity Catalog tables.
	"""
	out_tables = []
	items_properties = []

	gis = initialize_gis_connection(agol_inst, user, pswd)
	
	load_id = str(uuid.uuid4())
	_logger.info(f"Starting ingestion with load_id: {load_id}")

	for item_id in item_list:
		item = gis.content.get(item_id)
		_logger.info(f"Processing item: {item.title} (ID: {item_id})")

		dfs = {}
		if hasattr(item, 'layers') and item.layers:
			for layer in item.layers:
				layer_name_for_log = layer.properties.get(
					'name', layer.url.split('/')[-1]
				)
				sublayer_id_for_log = str(layer.properties.get('id', 'N/A'))

				# Generate cleaned table name
				table_name_parts = [
					p for p in [output_prefix, layer_name_for_log, output_postfix] 
					if p
				]
				out_table_name = "_".join(table_name_parts)
				out_table_name = re.sub(
					r'[^a-zA-Z0-9_]+', '_', out_table_name
				).strip('_').lower()[:255]
				
				target_path_for_log = f"{target_schema}.{out_table_name}"

				# Unique ID for this layer attempt
				now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
				hash_input = f"{target_path_for_log}-{now_str}"
				layer_attempt_id = hashlib.md5(
					hash_input.encode('utf-8')
				).hexdigest()

				# Initial log entry
				initial_log_entry = {
					"load_id": load_id,
					"layer_attempt_id": layer_attempt_id,
					"item_id": str(item_id),
					"sublayer_id": sublayer_id_for_log,
					"layer_url": str(layer.url),
					"layer_name": str(layer_name_for_log),
					"target_path": target_path_for_log,
					"timestamp": now_str,
					"status": "Pending",
					"error_message": None
				}

				_upsert_download_log_entry_(
					spark.createDataFrame(pd.DataFrame([initial_log_entry])), 
					log_table
				)

				status = 'Failed'
				error_message = None

				try:
					print(f'Reading {layer.url}')
					_df = spark.read \
						.format("feature-service") \
						.option("gis", "GIS") \
						.load(layer.url)
					
					_df = reproject_df(_df, out_cs=output_srid)

					layer.properties['out_table_name'] = out_table_name
					layer.properties['layer_attempt_id'] = layer_attempt_id
					dfs[layer.url] = {'df': _df, 'props': layer.properties}

					status = 'Read & Reprojected'
				except Exception as e:
					status = 'Failed'
					error_message = str(e)
					_logger.error(
						f"Failed to read/reproject layer {layer_name_for_log}: {e}"
					)

				# Update log entry post-read attempt
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
					spark.createDataFrame(pd.DataFrame([current_log_entry])), 
					log_table
				)

			# Meta-data formatting
			item.__dict__['dfs'] = dfs
			if 'description' in item.__dict__ and item.__dict__['description']:
				item.__dict__['description'] = html2text.html2text(
					item.__dict__['description']
				)
			else:
				item.__dict__['description'] = ""

			item.__dict__['created'] = datetime.fromtimestamp(
				item['created'] / 1000
			).strftime('%Y-%m-%d %H:%M:%S')
			
			item.__dict__['modified'] = datetime.fromtimestamp(
				item['modified'] / 1000
			).strftime('%Y-%m-%d %H:%M:%S')
			
			items_properties.append(item.__dict__)
		else:
			_logger.warning(f"Item '{item.title}' has no accessible layers.")

	# Process and write DataFrames to Unity Catalog
	for item in items_properties:
		for lyr_url, data in item['dfs'].items():
			_df = data['df']
			lyr_props = data['props']
			layer_name = lyr_props.get('name')
			out_table_name = lyr_props.get('out_table_name')
			table_path = f"{target_schema}.{out_table_name}"
			out_tables.append(table_path)
			
			layer_attempt_id = lyr_props.get('layer_attempt_id')
			if not layer_attempt_id:
				continue

			sr_info = get_spatial_reference_info(_df)
			geom_col = sr_info['Geometry Column']

			# Convert GAE Binary Geometry to standard WKT string via Python alias
			_df = _df.withColumn(geom_col, GAE.as_text(_df[geom_col]))
			_df = _df.withColumnRenamed(geom_col, "geometry_wkt")

			# Build description text
			lyr_desc_raw = lyr_props.get('description')
			final_desc = html2text.html2text(lyr_desc_raw) if lyr_desc_raw else ""
			if item['description'] and item['description'].strip():
				final_desc = (
					f"{final_desc}\n\n--- Parent Item Description ---\n"
					f"{item['description']}"
				)

			desc_text = (
				f"# Table Name : {out_table_name}\n"
				f"- ArcGIS Online ID: `{item['id']}`\n"
				f"- Description: {final_desc}\n"
				f"- Spatial Reference Properties:\n"
			)
			for k, v in sr_info.items():
				desc_text += f"\t- {k} : {v}\n"

			# Tagging logic
			combined_tags = list(item['tags'])
			layer_tags = lyr_props.get('tags', [])
			
			if isinstance(layer_tags, list):
				combined_tags.extend(layer_tags)
			else:
				combined_tags.append(layer_tags)
			
			final_tags = list(set([
				re.sub(r'[.,\-=/:]', '_', str(t))[:255] 
				for t in combined_tags if t
			]))[:50]

			log_status = "Complete"
			log_error = None

			try:
				_logger.info(f"Writing table: {table_path}")
				_df.write \
					.mode('overwrite') \
					.option('mergeSchema', 'true') \
					.saveAsTable(table_path)
				
				spark.sql(
					f"ALTER TABLE {table_path} "
					f"SET TBLPROPERTIES ('comment' = {repr(desc_text)})"
				)
				if final_tags:
					tag_sql = ", ".join([f"'{t}'" for t in final_tags])
					spark.sql(f"ALTER TABLE {table_path} SET TAGS ({tag_sql})")

			except Exception as e:
				log_status = "Failed"
				log_error = str(e)
				_logger.error(f"Error writing {table_path}: {e}")

			# Final log update
			final_log_entry = {
				"load_id": load_id,
				"layer_attempt_id": layer_attempt_id,
				"item_id": str(item['id']),
				"sublayer_id": str(lyr_props.get('id', 'N/A')),
				"layer_url": str(lyr_url),
				"layer_name": str(layer_name),
				"target_path": table_path,
				"timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'),
				"status": log_status,
				"error_message": log_error
			}
			
			_upsert_download_log_entry_(
				spark.createDataFrame(pd.DataFrame([final_log_entry])), 
				log_table
			)

	_logger.info(f"Ingestion process completed for load_id: {load_id}")
	return out_tables