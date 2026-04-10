---

### ga_engine_tools.utils.check_environment_compatibility

python
def check_environment_compatibility() -> None:
    """
    Verify compatibility between Databricks Runtime and GeoAnalytics Engine.

    Returns
    -------
    None
        Logs compatibility status and warnings if applicable.
    """
    current_dbr_version = None
    current_gae_version = None

    conf_key = "spark.databricks.clusterUsageTags.sparkVersion"
    dbr_spark_version_tag = spark.conf.get(conf_key, None)

    if dbr_spark_version_tag:
        dbr_version_parts = dbr_spark_version_tag.split(".")[0:2]
        current_dbr_version = parse_version(".".join(dbr_version_parts))

    try:
        gae_version_str = importlib.metadata.version("geoanalytics")
        current_gae_version = parse_version(".".join(gae_version_str.split('.')[:2]))
    except importlib.metadata.PackageNotFoundError:
        logger.debug("GeoAnalytics package not found.")

    if current_dbr_version and current_gae_version:
        if current_gae_version in GAE_DBR_COMPATIBILITY:
            min_dbr, max_dbr = GAE_DBR_COMPATIBILITY[current_gae_version]
            if not (min_dbr <= current_dbr_version <= max_dbr):
                logger.warning(
                    f"Compatibility Warning: GAE v{current_gae_version} is tested for DBR {min_dbr}-{max_dbr}. "
                    f"Current DBR: {current_dbr_version}."
                )
            else:
                logger.info(f"Environment Validated: GAE {current_gae_version} on DBR {current_dbr_version}.")


---

### ga_engine_tools.utils.initialize_gis_connection

python
def initialize_gis_connection(
    agol_inst: Optional[str] = None,
    user: Optional[str] = None,
    pswd: Optional[str] = None,
    verbose: bool = True
) -> Optional[GIS]:
    """
    Initialize and register GIS for GeoAnalytics Engine tools.

    Parameters
    ----------
    agol_inst : str, optional
        URL of the ArcGIS Online/Enterprise instance. Defaults to ArcGIS Online.
    user : str, optional
        Username for authentication.
    pswd : str, optional
        Password for authentication.
    verbose : bool, default True
        If True, prints status updates.

    Returns
    -------
    Optional[GIS]
        A connected GIS object, or None if authentication fails.
    """
    # 1. Block anonymous connections for GAE tasks
    if not user or not pswd:
        msg = "Authentication Error: Username and Password must be provided for GeoAnalytics Engine tasks."
        logger.error(msg)
        if verbose: print(f"ERROR: {msg}")
        return None

    try:
        target_url = agol_inst or "https://www.arcgis.com"
        if verbose:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to {target_url}...")
        
        # 2. Authenticate
        gis = GIS(url=target_url, username=user, password=pswd)
        
        # Verification: Check if connection is actually authenticated
        if gis.properties.get('isPortal') is None and not gis._con.token:
            raise ConnectionError("GIS Connection returned an anonymous session. Check your credentials.")

        # 3. GeoAnalytics Registration
        try:
            # Clean up any existing stale registration
            geoanalytics.unregister_gis("GIS")
        except:
            pass

        geoanalytics.register_gis("GIS", target_url, username=user, password=pswd)

        if verbose:
            print(f"Successfully registered identity: {gis.users.me.username}")
        return gis

    except Exception as e:
        error_msg = f"Failed to initialize GIS connection: {str(e)}"
        logger.error(error_msg)
        if verbose: print(f"ERROR: {error_msg}")
        return None


---

### ga_engine_tools.utils.get_spatial_reference_info

python
def get_spatial_reference_info(df: DataFrame) -> Dict[str, Any]:
    """
    Extract spatial reference details from a Spark DataFrame geometry field.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input Spark DataFrame containing spatial geometry.

    Returns
    -------
    dict
        Dictionary with spatial reference information, including geometry column,
        spatial reference WKID, WKT, projection status, units, and reference name.
    """
    try:
        geom_col = df.st.get_geometry_field()
        spatial_ref = df.st.get_spatial_reference()

        info_dict = {
            'Geometry Column': geom_col,
            'Spatial Reference WKID': spatial_ref.srid,
            'Spatial Reference WKT': spatial_ref.wkt,
            'Spatial Reference is Projected': spatial_ref.is_projected,
            'Spatial Reference Units': spatial_ref.unit,
        }

        # Extract Geographic Coordinate System name via Regex
        match = re.search(r'GEOGCS\[\s*"([^"]+)"', spatial_ref.wkt)
        info_dict['Spatial Reference Name'] = match.group(1) if match else "Unknown"
        return info_dict
    except Exception as e:
        logger.error(f"Failed to extract Spatial Reference: {e}")
        return {"Error": str(e)}


---

### ga_engine_tools.utils.reproject_df

python
def reproject_df(df: DataFrame, out_cs: int = 4326) -> DataFrame:
    """
    Reproject geometry using GAE ST_Transform. Defaults to WGS84 (4326).

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input Spark DataFrame containing spatial geometry.
    out_cs : int, optional
        Target spatial reference WKID. Defaults to 4326 (WGS84).

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with geometry reprojected to the target spatial reference.
    """
    geom_col = df.st.get_geometry_field()
    if not geom_col:
        logger.warning("Reprojection skipped: No geometry field found.")
        return df

    sr = df.st.get_spatial_reference()
    if sr.srid != out_cs:
        logger.info(f"Transforming {geom_col}: {sr.srid} -> {out_cs}")
        # Note: Using ST_Transform via expr is the standard GAE approach
        return df.withColumn(
            geom_col,
            F.expr(f"ST_Transform({geom_col}, {out_cs})")
        )
    
    return df


---

### ga_engine_tools.utils.fix_and_standardize_geometry

python
def fix_and_standardize_geometry(
    source_table: str,
    output_table: str,
    geom_col: str,
    tgt_srid: int = 4326
) -> str:
    """
    Repair and standardize geometries into GeoAnalytics binary types.

    Parameters
    ----------
    source_table : str
        Name of the source table containing spatial data.
    output_table : str
        Name of the output table to save standardized geometries.
    geom_col : str
        Name of the geometry column in the source table.
    tgt_srid : int, optional
        Target spatial reference WKID. Defaults to 4326.

    Returns
    -------
    str
        Name of the output table with standardized geometries.
    """
    logger.info(f"Standardizing geometry for table: {source_table}")
    df_spark = spark.read.table(source_table)

    # Standardize column naming and drop metadata columns often found in Esri exports
    drop_cols = [c for c in df_spark.columns if c.lower() in ['shape__area', 'shape__length']]
    df_spark_prep = df_spark.drop(*drop_cols)

    for c in df_spark_prep.columns:
        df_spark_prep = df_spark_prep.withColumnRenamed(c, c.lower())

    # Create a placeholder for the fixed WKT
    df_spark_prep = df_spark_prep.withColumn("wkt_fixed", F.lit(None).cast(StringType()))
    output_schema = df_spark_prep.schema
    output_cols = [field.name for field in output_schema]

    def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in iterator:
            if pdf.empty:
                yield pdf[output_cols]
                continue
            
            # Load WKT into Shapely
            pdf['temp_geom'] = pdf[geom_col].apply(
                lambda x: wkt.loads(str(x)) if pd.notnull(x) and str(x).strip() != "" else None
            )
            
            gdf = gpd.GeoDataFrame(pdf, geometry='temp_geom').dropna(subset=['temp_geom'])
            
            # Topology Repair: Buffer(0) is the standard 'Quick Fix' for self-intersections
            is_poly = gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])
            if is_poly.any():
                gdf.loc[is_poly, 'temp_geom'] = gdf.loc[is_poly, 'temp_geom'].buffer(0)
            
            # Explode multi-part geometries into single parts for GAE optimization
            gdf = gdf.explode(index_parts=False)
            
            valid_types = ['Polygon', 'MultiPolygon', 'Point', 'LineString']
            gdf = gdf[gdf.geometry.type.isin(valid_types) & (~gdf.is_empty)]
            
            gdf['wkt_fixed'] = gdf.geometry.to_wkt(rounding_precision=8)
            
            final_pdf = pd.DataFrame(gdf.drop(columns=['temp_geom']))
            final_pdf.columns = [col.lower() for col in final_pdf.columns]
            yield final_pdf[output_cols]

    # Execute Python logic across Spark Executors
    fixed_spark_df = df_spark_prep.mapInPandas(process_partition, schema=output_schema)
    
    # Cast fixed WKT strings back to GAE Binary Geometries
    sql_geom = f"ST_Transform(ST_GeomFromText(wkt_fixed, 4326), {tgt_srid})"
    
    final_df = fixed_spark_df.selectExpr("*", f"{sql_geom} AS geometry").drop("wkt_fixed", geom_col.lower())
    
    final_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(output_table)
    spark.sql(f"OPTIMIZE {output_table}")
    
    return output_table


---

### ga_engine_tools.utils.check_st_native_status

python
def check_st_native_status(st_functions_to_test: List[str]) -> None:
    """
    Diagnostic tool to verify if ST_ functions are powered by Databricks or ESRI.

    Parameters
    ----------
    st_functions_to_test : list of str
        List of spatial function names to check.

    Returns
    -------
    None
        Prints the ownership status of each function.
    """
    print(f"{'Function':<15} | {'Owner / Class Path'}")
    print("-" * 60)

    for f in

Repair and standardize geometries into GeoAnalytics binary types.

---

### ga_engine_tools.utils.get_spatial_reference_info

Extract spatial reference details from a Spark DataFrame geometry field.

---

### ga_engine_tools.utils.initialize_gis_connection

Initialize and register GIS for GeoAnalytics Engine tools.

**Parameters**
----------
- `agol_inst`: str, optional  
  URL of the ArcGIS Online/Enterprise instance. Defaults to ArcGIS Online.
- `user`: str, optional  
  Username for authentication.
- `pswd`: str, optional  
  Password for authentication.
- `verbose`: bool, default True  
  If True, prints status updates.

**Returns**
-------
Optional[GIS]  
A connected GIS object, or None if authentication fails.

---

### ga_engine_tools.utils.parse_version

Parse the given version string.

python
>>> parse('1.0.dev1')
<Version('1.0.dev1')>


:param version: The version string to parse.  
:raises InvalidVersion: When the version string is not a valid version.

---

### ga_engine_tools.utils.reproject_df

Reproject geometry using GAE ST_Transform. Defaults to WGS84 (4326).

---
