package org.geotools.data.bigquery;

public enum BigqueryAccessMethod {
    /**
     * https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1
     *
     * <p>Faster access to storage, less flexibility.
     */
    STORAGE_API,

    /**
     * https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/overview
     *
     * <p>Slower, more flexible in ability to simplify and perform other in-database calculations.
     */
    STANDARD_QUERY_API
}
