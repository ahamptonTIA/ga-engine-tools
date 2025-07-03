# ga-engine-tools

A Python library offering a collection of utilities for common data operations, with a primary focus on managing ArcGIS GIS connections and robustly transferring large-scale data to and from ArcGIS Online using PySpark.

## Overview

`ga-engine-tools` is designed to simplify interactions between Databricks (and other PySpark environments) and ArcGIS Online. It provides:

* **Environment Compatibility Checks:** Automatic verification of Databricks Runtime and ArcGIS GeoAnalytics Engine compatibility on package import, helping to prevent common environment-related issues. For detailed information on supported runtimes and GAE versions, please refer to the [official ArcGIS GeoAnalytics Engine Databricks documentation](https://developers.arcgis.com/geoanalytics/install/databricks/).
* **AGOL Data Export (`agol_data_export`):** Utilities for efficiently exporting PySpark DataFrames (containing features or tabular data) to ArcGIS Online hosted feature services or tables. This module handles common challenges like chunking, retries, and error logging for robust publication of large datasets.
* **AGOL Data Import (`agol_data_import`):** Utilities for robustly importing data (features or tabular data) from ArcGIS Online hosted items (Feature Layers or Hosted Tables) into PySpark DataFrames. It supports querying and filtering for selective data retrieval.
* **General Utilities (`utils`):** A collection of helper functions for common tasks within a Databricks/ArcGIS ecosystem.

### Package Structure

```
ga-engine-tools/
├── ga_engine_tools/
│   ├── __init__.py
│   ├── agol_data_export.py   <-- Push data from PySpark DataFrames to ArcGIS Online
│   ├── agol_data_import.py   <-- Retrieve data and metadata from ArcGIS Online to PySpark DataFrames
│   └── utils.py              <-- Generalized helper classes and functions for GeoAnalytics workflows
├── README.md
├── LICENSE.md
└── setup.py
```

## Installation

This package is primarily intended for use in a Databricks environment or any PySpark-enabled environment where the ArcGIS API for Python and ArcGIS GeoAnalytics Engine are available.

### Prerequisites

Before installing `ga-engine-tools`, ensure your environment has:

* **PySpark:** A running Spark cluster (e.g., Databricks, AWS EMR, local Spark).
* **ArcGIS API for Python:** (`arcgis` package).
* **ArcGIS GeoAnalytics Engine:** (`geoanalytics` package). For detailed installation steps and compatibility information with Databricks Runtimes, please consult the [official ArcGIS GeoAnalytics Engine Databricks documentation](https://developers.arcgis.com/geoanalytics/install/databricks/).
* **`databricks_helper`:** If leveraging Databricks-specific functionalities (e.g., Delta Lake interactions).

### Using `pip install` 

To install the package directly from the GitHub repository in DataBricks:
```
%pip install -e git+https://github.com/ahamptonTIA/ga-engine-tools.git
```

```bash
pip install -e git+https://github.com/ahamptonTIA/ga-engine-tools.git#egg=ga-engine-tools
```

---

## Usage Examples

### 1. Import AGOL Feature Layers to Unity Catalog

**Function:** `ingest_agol_items_to_unity_catalog`

```python
from ga_engine_tools.agol_data_import import ingest_agol_items_to_unity_catalog

# List of AGOL item IDs (Feature Service IDs)
item_list = ["abcdef1234567890abcdef1234567890", "abcdef1234567890abcdef1234567890"]

# Ingest all layers in each item to Databricks Unity Catalog under the target schema
written_tables = ingest_agol_items_to_unity_catalog(
    item_list=item_list,
    target_schema="mycatalog.myschema",
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>",
    output_srid=4326,  # Optional: spatial reference ID to project to
    log_table="mycatalog.logs.agol_import_log",  # Optional: log table path
    output_prefix="imported",  # Optional: add prefix to table names
)
print(written_tables)
```
*Bulk-ingest one or more ArcGIS Online Feature Service items (by item ID) into Databricks Unity Catalog tables, 
optionally logging each step and reprojection.*

---

### 2. Update ArcGIS Feature Service with DataFrame

**Function:** `update_arcgis_feature_service`

```python
from ga_engine_tools.agol_data_export import update_arcgis_feature_service

# DataFrame 'df' must exist in your Spark session with a batch column
# that partitions or groups the DataFrame to be written in chunks, e.g., 'state_county_fips'
result = update_arcgis_feature_service(
    input_df=df,
    target_svc_name="my_feature_service",
    target_server_url="https://services.arcgis.com/ORGID/arcgis/rest/services",
    batch_by_column_name="state_county_fips",
    layer_name="state__county_features",  # Output Feature Layer/service name
    write_mode="overwrite",
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>",
    logTable="mycatalog.logs.export_log",  # Optional
    service_summary="Automated feature update.", # Optional
    service_tags=["automated", "update"], # Optional
)
print("Update status?", result)
```
*Writes/updates a Spark DataFrame to an ArcGIS Online Feature Service using batching and retries, ensuring that the service matches your DataFrame.*

---

### 3. Get Spatial Reference Information

**Function:** `get_spatial_reference_info`

```python
from ga_engine_tools.utils import get_spatial_reference_info

sr_info = get_spatial_reference_info(df)
print(sr_info)
# Output Example: {
#     'Geometry Column': 'geometry',
#     'Spatial Reference WKID': 4326,
#     'Spatial Reference WKT': 'GEOGCS["WGS 84", ...]',
#     'Spatial Reference is Projected' : 'True',
#     'Spatial Reference Units' : 'Degrees',
#     'Spatial Reference Name' : 'GCS_WGS_84',
#     }
```
*Extracts and displays spatial reference (SRID, WKT, name, units, etc.) for the geometry column in a Spark DataFrame.*

---

### 4. Reproject a DataFrame

**Function:** `reproject_df`

```python
from ga_engine_tools.utils import reproject_df

df_reprojected = reproject_df(df, out_cs=4326)  # Reproject to Web Mercator (EPSG:3857)

# Output Example Message:
# Transforming Spatial Reference
# 		-Input Projection:
# 		-SRID: 4326
# 		-Projected: False
# 		-Unit: Degree

# 	-Transformed To
# 		-Output Projection:
# 		-SRID: 3857
# 		-Projected: True
# 		-Unit: Meter
```
*Detects the geometry column and current spatial reference system and reprojects the Spark DataFrame to a new spatial reference system (SRID).*

---

### 5. Check Databricks/GeoAnalytics Compatibility

**Function:** `check_environment_compatibility`

```python
from ga_engine_tools.utils import check_environment_compatibility

check_environment_compatibility()
# Will print compatibility warnings or confirmation to the notebook or logs.
```
*Checks if the current Databricks Runtime and ArcGIS GeoAnalytics Engine versions are compatible and prints a warning if not.*

---

### 6. Initialize ArcGIS Connection for GeoAnalytics

**Function:** `initialize_gis_connection`

```python
from ga_engine_tools.utils import initialize_gis_connection

gis = initialize_gis_connection(
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>"
)
```
*Creates and registers an authenticated connection to ArcGIS Online/Enterprise for both the ArcGIS API for Python and GeoAnalytics Engine.*

---

## Function Summaries

| Function | Purpose |
|----------|---------|
| `ingest_agol_items_to_unity_catalog` | Ingest ArcGIS Online feature layers into Databricks Unity Catalog tables with logging and reprojection support. |
| `update_arcgis_feature_service` | Write/update a Spark DataFrame to an ArcGIS Online Feature Service with batching and automatic retries. |
| `get_spatial_reference_info` | Extract spatial reference details (SRID, WKT, etc.) from a DataFrame. |
| `reproject_df` | Reproject a DataFrame’s geometry column to a different spatial reference. |
| `check_environment_compatibility` | Verify that your Databricks and GeoAnalytics Engine versions are compatible. |
| `initialize_gis_connection` | Authenticate and configure both the ArcGIS Python API and GeoAnalytics Engine for your session. |

---
