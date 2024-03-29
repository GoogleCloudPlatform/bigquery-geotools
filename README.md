# bigquery-geotools

Add [BigQuery](https://cloud.google.com/bigquery) support to [Geoserver](https://geoserver.org/).

<img src="https://storage.googleapis.com/bigquery-geotools-public/new_bq_datasource.png" width=640>

## Install

Build or download the JARs and copy them into geoserver's `WEB-INF/lib` folder in the same way you install other geoserver plugins.

### Download

A `.zip` containing the `bigquery-geotools` JAR and required dependency JARs is built against JDK 8 is available in under [releases](https://github.com/GoogleCloudPlatform/bigquery-geotools/releases).

### Build

1. Use Cloud Build
```
gcloud builds submit .
```

2. Build locally. Requires JDK 8+.

```
mvn package -DskipTests
```

## Authentication

The service account used to authenticate with BigQuery needs the following permissions:

- bigquery.jobs.create
- bigquery.tables.get
- bigquery.tables.getData
- bigquery.tables.lister

You can use one of three authentication methods to connect to BigQuery, depending on your requirements:

1. If your geoserver instance is running inside GCP, on a VM or elsewhere, you can make use of [Default service account credentials](https://cloud.google.com/compute/docs/access/service-accounts#default_service_account). (Recommended)

2. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable

3. Export a service account key JSON file, add it to disk, and manually select the file when configuring the BigQuery data source.
*Note*: Do not store the service account key file in the geoserver data directory, it may not be secure there.

## Usage

### 1. Create a new Vector source 
Create a new data soruce he same way as other databases. Under `Stores`, select `Add New Store`, then select `BigQuery Table`.

### 2. Configure the Vector Source

Now, you will configure the connection to a [BigQuery dataset](https://cloud.google.com/bigquery/docs/datasets-intro). Datasets can contain many tables, and are roughly analogous to [PostGIS schemas](https://postgis.net/workshops/postgis-intro/schemas.html).

#### Pregenerate Materialized Views

The `Pregenerate Materialized Views` setting will do one of the following:
- `MV_NONE`: do not generate or utilize any pregenerated materialized views.
- `MV_USE_EXISTING`: utilize any [existing materialized views](https://github.com/GoogleCloudPlatform/bigquery-geotools/blob/main/src/sample/create_pregen_views.sql), but do not generate any. 
- `MV_PREGEN_ALL`: pregenerate 4 tables of simplified geometries at tolerances `1m`, `10m`, `100m`, and `1000m`.

To generate materialized views manually, see this [sample SQL](https://github.com/GoogleCloudPlatform/bigquery-geotools/blob/main/src/sample/create_pregen_views.sql).


<img src="https://storage.googleapis.com/bigquery-geotools-public/config_bq_datasource_1.png" width=480>

### 3. Add and configure layer

Select `Publish` to configure and publish a vectory layer from BigQuery.

<img src="https://storage.googleapis.com/bigquery-geotools-public/new_layer.png" width=640>


## Configuration

There are several configuration options that can be set when adding a BigQuery data source.

| Parameter | Default | Description |
|----|----|----|
| Access Method | `QUERY_API` | Select whether to query using the BigQuery [Storage API](https://cloud.google.com/bigquery/docs/reference/storage) or [standard Query API](https://cloud.google.com/bigquery/docs/reference/rest). |
| Simplify Geometries | `true` | Attempt to simplify geometries at wider zoom levels without impacting accuracy |
| Use Query Cache | `true` | Use the [BigQuery query cache](https://cloud.google.com/bigquery/docs/cached-results) when possible |
| Query Recent Partition | `true` | When querying a partitioned table, automatically detect the most recent partition and use it for queries |


