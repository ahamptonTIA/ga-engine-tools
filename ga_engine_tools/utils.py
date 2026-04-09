import re
import logging
import importlib.metadata
from typing import Iterator, Optional, Dict, Any, List

# Third-party GIS and Data Analysis
import pandas as pd
import geopandas as gpd
from shapely import wkt
from packaging.version import parse as parse_version

# Esri core (connectivity and data loading)
import geoanalytics
# Aliased to GAE to facilitate the .as_text() conversion for WKT output
from geoanalytics.sql import functions as GAE 

from arcgis.gis import GIS

# Databricks & Spark native
from databricks.sdk.runtime import spark
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import StringType

# -----------------------------------------------------------------------------

# Use __name__ so logs are categorized by this specific submodule
_logger = logging.getLogger(__name__)

# Updated compatibility mapping for GAE 2.0 (March 2026)
GAE_DBR_COMPATIBILITY = {
	parse_version("1.1"): (parse_version("7.3"), parse_version("12.9")),
	parse_version("1.2"): (parse_version("7.3"), parse_version("13.9")),
	parse_version("1.3"): (parse_version("9.1"), parse_version("14.9")),
	parse_version("1.4"): (parse_version("9.1"), parse_version("15.9")),
	parse_version("1.5"): (parse_version("11.3"), parse_version("15.9")),
	parse_version("1.6"): (parse_version("11.3"), parse_version("16.9")),
	parse_version("1.7"): (parse_version("12.2"), parse_version("17.9")),
	parse_version("2.0"): (parse_version("13.3"), parse_version("18.9")),
}

# -----------------------------------------------------------------------------

def check_environment_compatibility() -> None:
	"""
	Verify compatibility between Databricks Runtime and GeoAnalytics Engine.
	"""
	current_dbr_version = None
	current_gae_version = None

	conf_key = "spark.databricks.clusterUsageTags.sparkVersion"
	dbr_spark_version_tag = spark.conf.get(conf_key, None)

	if dbr_spark_version_tag:
		dbr_version_parts = dbr_spark_version_tag.split(".")[0:2]
		current_dbr_version = parse_version(".".join(dbr_version_parts))

	if geoanalytics:
		try:
			gae_version_str = importlib.metadata.version("geoanalytics")
			current_gae_version = parse_version(
				".".join(gae_version_str.split('.')[:2])
			)
		except Exception:
			pass

	if current_dbr_version and current_gae_version:
		if current_gae_version in GAE_DBR_COMPATIBILITY:
			min_dbr, max_dbr = GAE_DBR_COMPATIBILITY[current_gae_version]
			if not (min_dbr <= current_dbr_version <= max_dbr):
				_logger.warning(
					f"GAE v{current_gae_version} mismatch with DBR {current_dbr_version}."
				)
			else:
				print(
					f"Compatibility check passed: GAE {current_gae_version} "
					f"on DBR {current_dbr_version}."
				)
	else:
		_logger.debug("Compatibility check skipped: Version info missing.")

# -----------------------------------------------------------------------------

def geoanalytics_unregister() -> None:
	"""
	Unregister the existing GIS configuration from geoanalytics.
	"""
	try:
		geoanalytics.unregister_gis("GIS")
	except Exception:
		pass

# -----------------------------------------------------------------------------

def initialize_gis_connection(
	agol_inst: str,
	user: str,
	pswd: str,
	print_status: bool = True
) -> Optional[GIS]:
	"""
	Initialize and register GIS for GeoAnalytics Engine tools.
	"""
	try:
		if print_status:
			print("\t-Setting ArcGIS Online Connections")
		
		gis = GIS(url=agol_inst, username=user, password=pswd)

		# Ensure clean state before registration
		geoanalytics_unregister()
		geoanalytics.register_gis("GIS", agol_inst, username=user, password=pswd)

		if print_status:
			print("\t-Successfully registered GIS connection.")
		return gis
	except Exception as e:
		if print_status:
			print(f"Failed to connect: {e}")
		return None

# -----------------------------------------------------------------------------

def get_spatial_reference_info(df: DataFrame) -> Dict[str, Any]:
	"""
	Extract spatial reference details from a Spark DataFrame.
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

	match = re.search(r'GEOGCS\[\s*"([^"]+)"', spatial_ref.wkt)
	info_dict['Spatial Reference Name'] = match.group(1) if match else None
	return info_dict

# -----------------------------------------------------------------------------

def reproject_df(df: DataFrame, out_cs: int = 3857) -> DataFrame:
	"""
	Reproject geometry using the GeoAnalytics Engine ST_Transform function.
	"""
	geom_col = df.st.get_geometry_field()
	if not geom_col:
		_logger.error("DataFrame has no defined geometry/spatial reference")
		return df

	sr = df.st.get_spatial_reference()
	if sr.srid != out_cs:
		print(f"Transforming {sr.srid} -> {out_cs} using GAE SQL...")
		return df.withColumn(
			geom_col,
			F.expr(f"ST_Transform({geom_col}, {out_cs})")
		)
	
	print(f"No transformation required. Already: {out_cs}")
	return df

# -----------------------------------------------------------------------------

def prepare_target_points(
	table_name: str,
	input_srid: int = 4326,
	output_srid: Optional[int] = None,
	longitude_col: str = "longitude",
	latitude_col: str = "latitude"
) -> DataFrame:
	"""
	Load points and define/transform them into GAE geometry types.
	"""
	_logger.info(f"Loading {table_name} (Input SRID: {input_srid})")

	df = spark.read.table(table_name).withColumn(
		"wkt_string",
		F.concat(F.lit("POINT ("), F.col(longitude_col), F.lit(" "), F.col(latitude_col), F.lit(")"))
	)

	geom_expr = f"ST_GeomFromText(wkt_string, {input_srid})"
	if output_srid and output_srid != input_srid:
		geom_expr = f"ST_Transform({geom_expr}, {output_srid})"

	return df.withColumn("geometry", F.expr(geom_expr)).select(
		"location_id", longitude_col, latitude_col, "geometry"
	)

# -----------------------------------------------------------------------------

def fix_and_standardize_geometry(
	source_table: str,
	output_table: str,
	geom_col: str,
	tgt_srid: int = 4326
) -> str:
	"""
	Repair and standardize geometries into GeoAnalytics Engine binary types.
	"""
	_logger.info(f"Reading source table: {source_table}")
	df_spark = spark.read.table(source_table)

	# Clean columns
	drop_cols = [c for c in df_spark.columns if c.lower() in ['shape__area', 'shape__length']]
	df_spark_prep = df_spark.drop(*drop_cols)

	for c in df_spark_prep.columns:
		df_spark_prep = df_spark_prep.withColumnRenamed(c, c.lower())

	df_spark_prep = df_spark_prep.withColumn("wkt_fixed", F.lit(None).cast(StringType()))
	output_schema = df_spark_prep.schema
	output_cols = [field.name for field in output_schema]

	def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
		for pdf in iterator:
			if pdf.empty:
				yield pdf[output_cols]
				continue
			pdf['temp_geom'] = pdf[geom_col].apply(
				lambda x: wkt.loads(x) if pd.notnull(x) and str(x).strip() != "" else None
			)
			gdf = gpd.GeoDataFrame(pdf, geometry='temp_geom').dropna(subset=['temp_geom'])
			is_poly = gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])
			if is_poly.any():
				gdf.loc[is_poly, 'temp_geom'] = gdf.loc[is_poly, 'temp_geom'].buffer(0)
			gdf = gdf.explode(index_parts=False)
			valid_types = ['Polygon', 'MultiPolygon', 'Point', 'LineString']
			gdf = gdf[gdf.geometry.type.isin(valid_types) & (~gdf.is_empty)]
			gdf['wkt_fixed'] = gdf.geometry.to_wkt(rounding_precision=8)
			final_pdf = pd.DataFrame(gdf.drop(columns=['temp_geom']))
			final_pdf.columns = [col.lower() for col in final_pdf.columns]
			yield final_pdf[output_cols]

	fixed_spark_df = df_spark_prep.mapInPandas(process_partition, schema=output_schema)
	sql_geom = f"ST_Transform(ST_GeomFromText(wkt_fixed, 4326), {tgt_srid})"
	
	final_df = fixed_spark_df.selectExpr("*", f"{sql_geom} AS geometry").drop("wkt_fixed", geom_col)
	final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(output_table)
	spark.sql(f"OPTIMIZE {output_table}")
	
	return output_table

# -----------------------------------------------------------------------------

def check_st_native_status(st_functions_to_test: List[str]) -> None:
	"""
	Diagnostic tool to check which engine (Databricks vs ESRI) owns SQL functions.
	"""
	print(f"{'Function':<15} | {'Owner / Class Path'}")
	print("-" * 80)

	for f in st_functions_to_test:
		try:
			rows = spark.sql(f"DESCRIBE FUNCTION EXTENDED {f}").collect()
			class_info = next((r[0] for r in rows if "Class:" in r[0]), "Unknown")
			
			if "databricks" in class_info.lower():
				status = "🟧 Databricks Native"
			elif "esri" in class_info.lower():
				status = "🌐 ESRI GAE"
			else:
				status = f"❓ {class_info}"

			print(f"{f:<15} | {status}")
		except Exception as e:
			print(f"{f:<15} | ⚠️ Not Found: {str(e)[:40]}")