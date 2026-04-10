# ga-engine-tools

A Python library offering robust utilities for data operations, with a primary focus on managing ArcGIS GIS connections and efficiently transferring large-scale data to and from ArcGIS Online using PySpark.

---

## 📚 Overview

`ga-engine-tools` streamlines interactions between Databricks (and other PySpark environments) and ArcGIS Online. It provides:

- **Environment Compatibility Checks:** Automatic verification of Databricks Runtime and ArcGIS GeoAnalytics Engine compatibility.
- **AGOL Data Export:** Utilities for exporting PySpark DataFrames to ArcGIS Online hosted feature services or tables.
- **AGOL Data Import:** Utilities for importing data from ArcGIS Online hosted items into PySpark DataFrames.
- **General Utilities:** Helper functions for common tasks within Databricks/ArcGIS workflows.

---

## 🗂️ Package Structure


ga-engine-tools/  
├── ga_engine_tools/  
│   ├── __init__.py  
│   ├── agol_data_export.py   # Export DataFrames to ArcGIS Online  
│   ├── agol_data_import.py   # Import data from ArcGIS Online to DataFrames  
│   └── utils.py              # Helper functions for GeoAnalytics workflows  
├── README.md  
├── LICENSE.md  
└── setup.py


---

## ⚡ Installation

This package is intended for Databricks or any PySpark-enabled environment with ArcGIS API for Python and ArcGIS GeoAnalytics Engine.

### Prerequisites

- **PySpark:** Running Spark cluster (Databricks, AWS EMR, local Spark).
- **ArcGIS API for Python:** (`arcgis` package).
- **ArcGIS GeoAnalytics Engine:** (`geoanalytics` package).
- **`databricks_helper`:** For Databricks-specific functionalities.

### Install via pip

bash
%pip install git+https://github.com/ahamptonTIA/ga-engine-tools.git


---

## 🚀 Usage Examples & Function Documentation

### agol_data_import.py

#### `ingest_agol_items_to_unity_catalog`

Bulk-ingest ArcGIS Online Feature Service items (by item ID) into Databricks Unity Catalog tables, with optional logging and reprojection.

python
from ga_engine_tools.agol_data_import import ingest_agol_items_to_unity_catalog

item_list = ["abcdef1234567890abcdef1234567890", "abcdef1234567890abcdef1234567890"]
written_tables = ingest_agol_items_to_unity_catalog(
    item_list=item_list,
    target_schema="mycatalog.myschema",
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>",
    output_srid=4326,
    log_table="mycatalog.logs.agol_import_log",
    output_prefix="imported",
)
print(written_tables)


**Parameters:**
- `item_list`: List of AGOL item IDs.
- `target_schema`: Unity Catalog schema path.
- `agol_inst`: AGOL instance URL.
- `user`, `pswd`: Credentials.
- `output_srid`: Optional spatial reference ID.
- `log_table`: Optional log table path.
- `output_prefix`: Optional table name prefix.

---

### agol_data_export.py

#### `update_arcgis_feature_service`

Write/update a Spark DataFrame to an ArcGIS Online Feature Service using batching and retries.

python
from ga_engine_tools.agol_data_export import update_arcgis_feature_service

result = update_arcgis_feature_service(
    input_df=df,
    target_svc_name="my_feature_service",
    target_server_url="https://services.arcgis.com/ORGID/arcgis/rest/services",
    batch_by_column_name="state_county_fips",
    layer_name="state__county_features",
    write_mode="overwrite",
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>",
    logTable="mycatalog.logs.export_log",
    service_summary="Automated feature update.",
    service_tags=["automated", "update"],
)
print("Update status?", result)


**Parameters:**
- `input_df`: Spark DataFrame to export.
- `target_svc_name`: Feature Service name.
- `target_server_url`: AGOL REST endpoint.
- `batch_by_column_name`: Column for batching.
- `layer_name`: Output layer name.
- `write_mode`: "overwrite" or "append".
- `agol_inst`, `user`, `pswd`: AGOL credentials.
- `logTable`: Optional log table.
- `service_summary`, `service_tags`: Optional metadata.

---

### utils.py

#### `get_spatial_reference_info`

Extract spatial reference details (SRID, WKT, name, units, etc.) from a DataFrame.

python
from ga_engine_tools.utils import get_spatial_reference_info

sr_info = get_spatial_reference_info(df)
print(sr_info)


**Returns:**  
Dictionary with geometry column, SRID, WKT, projection status, units, and name.

---

#### `reproject_df`

Reproject a DataFrame’s geometry column to a different spatial reference.

python
from ga_engine_tools.utils import reproject_df

df_reprojected = reproject_df(df, out_cs=4326)


**Parameters:**
- `df`: Input Spark DataFrame.
- `out_cs`: Output SRID.

---

#### `check_environment_compatibility`

Verify Databricks and GeoAnalytics Engine version compatibility.

python
from ga_engine_tools.utils import check_environment_compatibility

check_environment_compatibility()


**Returns:**  
Prints compatibility warnings or confirmation.

---

#### `initialize_gis_connection`

Authenticate and configure ArcGIS Python API and GeoAnalytics Engine for your session.

python
from ga_engine_tools.utils import initialize_gis_connection

gis = initialize_gis_connection(
    agol_inst="https://www.arcgis.com",
    user="<username>",
    pswd="<password>"
)


**Returns:**  
Authenticated GIS connection object.

---

#### `get_geometry_column`

Detects the geometry column in a Spark DataFrame.

python
from ga_engine_tools.utils import get_geometry_column

geom_col = get_geometry_column(df)
print("Geometry column:", geom_col)


---

#### `log_to_table`

Log operation details to a Databricks table.

python
from ga_engine_tools.utils import log_to_table

log_to_table(
    log_table="mycatalog.logs.operation_log",
    log_dict={"operation": "import", "status": "success"}
)


---

### __init__.py

- Imports and exposes main modules and functions for easy access.

---

## 📝 Function Summaries

| Function | Purpose |
|----------|---------|
| `ingest_agol_items_to_unity_catalog` | Ingest AGOL feature layers into Unity Catalog tables with logging and reprojection. |
| `update_arcgis_feature_service` | Write/update DataFrame to AGOL Feature Service with batching and retries. |
| `get_spatial_reference_info` | Extract spatial reference details from DataFrame. |
| `reproject_df` | Reproject DataFrame’s geometry column. |
| `check_environment_compatibility` | Verify Databricks and GeoAnalytics Engine compatibility. |
| `initialize_gis_connection` | Authenticate and configure ArcGIS API and GeoAnalytics Engine. |
| `get_geometry_column` | Detect geometry column in DataFrame. |
| `log_to_table` | Log operation details to Databricks table. |

---