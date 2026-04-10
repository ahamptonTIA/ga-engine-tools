
-----

# ga-engine-tools

[](https://www.python.org/downloads/)
[](https://spark.apache.org/)
[](https://opensource.org/licenses/MIT)

A  Python library providing a helper functions between **Databricks (PySpark)** and **ESRI GeoAnalytics Engine (GAE)**. This toolkit simplifies managing GIS connections, ensures environment compatibility, and performs large-scale data transfers using ArcGIS Online and GeoAnalytics Engine.

-----

## 🚀 Key Features

  * **Environment Validation:** Automatic runtime checks to ensure Databricks and ArcGIS GeoAnalytics Engine compatibility.
  * **High-Volume Export:** Orchestrated exports with batching, chunking, and automatic retries.
  * **Ingestion:** Rapidly pull AGOL layers into Databricks Unity Catalog with metadata preservation.
  * **Spatial Integrity:** Repair and standardize multi-part geometries for better partitioning.
  * **Audit Logging:** Integrated Delta Lake logging for tracking data movement.

-----

## 📁 Project Structure

```text
ga-engine-tools 
├── ga_engine_tools
│   ├── __init__.py
│   ├── agol_data_export.py
│   ├── agol_data_import.py
│   └── utils.py              
├── tests                    
└── pyproject.toml            
```

-----

## 🛠 Function Summary

### Data Ingestion (`agol_data_import`)

| Function | Description |
| :--- | :--- |
| `ingest_agol_items_to_unity_catalog` | Bulk-ingest AGOL layers into Databricks Delta tables. |
| `_upsert_download_log_entry` | Internal helper to manage Delta log entries for downloads. |

### Data Publication (`agol_data_export`)

| Function | Description |
| :--- | :--- |
| `update_arcgis_feature_service` | Orchestrates high-volume exports with batching and retries. |
| `write_df_in_chunks` | Writes DataFrames to Feature Services using row-based chunking. |
| `set_pyspark_schema_from_feature_service` | Casts Spark DF schema to match a target AGOL Feature Service. |
| `check_create_feature_service` | Verifies existence of or creates a new Feature Service in AGOL. |
| `_feature_service_log` | Logs batch processing status to a Delta table for auditing. |

### Spatial & Environment Utilities (`utils`)

| Function | Description |
| :--- | :--- |
| `initialize_gis_connection` | Authenticates both ArcGIS Python API and GeoAnalytics Engine. |
| `fix_and_standardize_geometry` | Repairs and standardizes geometries to GeoAnalytics types. |
| `reproject_df` | Projects a DataFrame to a new coordinate system using `ST_Transform`. |
| `get_spatial_reference_info` | Extracts detailed CRS metadata (WKID, WKT, Units) from a DF. |
| `check_environment_compatibility` | Validates Databricks Runtime vs. GeoAnalytics Engine versions. |
| `check_st_native_status` | Identifies if `ST_` functions are provided by Databricks or ESRI. |
| `parse_version` | Validates version strings for package management. |

-----

## 📖 Usage Examples

### 1\. High-Volume Export with Logging

The `update_arcgis_feature_service` function is the primary entry point for moving large datasets from Spark to ArcGIS.

```python
from ga_engine_tools.agol_data_export import update_arcgis_feature_service

result = update_arcgis_feature_service(
    input_df=df,
    target_svc_name="regional_assets_service",
    batch_col="county_fips",
    write_mode="overwrite",
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>",
    log_table="audit.logs.export_history"
)
```

### 2\. Geometry Standardization & Repair

Ensure your Delta tables are optimized for GeoAnalytics before performing spatial joins.

```python
from ga_engine_tools.utils import fix_and_standardize_geometry

fix_and_standardize_geometry(
    source_table="raw.gis.parcel_data",
    output_table="curated.gis.parcel_standardized",
    geom_col="shape",
    tgt_srid=4326
)
```

### 3\. Schema Alignment

Prevent export failures by casting your Spark DataFrame to the exact schema of an existing Feature Service.

```python
from ga_engine_tools.agol_data_export import set_pyspark_schema_from_feature_service
from arcgis.gis import GIS

gis = GIS("https://www.arcgis.com", "user", "pass")
df_aligned = set_pyspark_schema_from_feature_service(
    input_df=df,
    layer_url="https://services.arcgis.com/.../FeatureServer/0",
    gis_object=gis
)
```

-----

## ⚙️ Installation

```bash
# Inside a Databricks Notebook
%pip install git+https://github.com/ahamptonTIA/ga-engine-tools.git
```

## 🤝 Contributing

Please open an issue to discuss proposed changes before submitting a Pull Request. 

## 📄 License

Distributed under the MIT License.