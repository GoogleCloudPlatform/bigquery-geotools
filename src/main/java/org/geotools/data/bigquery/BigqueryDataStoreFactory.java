package org.geotools.data.bigquery;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.KVP;
import org.geotools.util.logging.Logging;

public class BigqueryDataStoreFactory implements DataStoreFactorySpi {
    private static final Logger LOGGER = Logging.getLogger(BigqueryDataStoreFactory.class);

    public static final Param PROJECT_ID =
            new Param("Project Id", String.class, "GCP Project ID", true, null);

    public static final Param DATASET_NAME =
            new Param("Dataset Name", String.class, "BigQuery Dataset Name", true, null);

    public static final Param SERVICE_ACCOUNT_KEY_FILE =
            new Param(
                    "Service Account Key File",
                    File.class,
                    "Service Account Key File",
                    false,
                    null);

    public static final Param ACCESS_METHOD =
            new Param(
                    "Access Method",
                    BigqueryAccessMethod.class,
                    "Access BigQuery using the Storage API, or the standard BigQuery API",
                    false,
                    BigqueryAccessMethod.STANDARD_QUERY_API,
                    new KVP(Param.OPTIONS, Arrays.asList(BigqueryAccessMethod.values())));

    public static final Param SIMPLIFY =
            new Param(
                    "Simplify Geometries in BigQuery",
                    Boolean.class,
                    "Use BigQuery's ST_SIMPLIFY function (applicable to STANDARD_QUERY_API)",
                    false,
                    true);

    public static final Param USE_QUERY_CACHE =
            new Param(
                    "Use Query Cache",
                    Boolean.class,
                    "Use temporary query cache in BigQuery (applicable to STANDARD_QUERY_API)",
                    false,
                    true);

    public static final Param AUTO_ADD_PARTITION_FILTER =
            new Param(
                    "Automatically query most recent Partition",
                    Boolean.class,
                    "Whether to auto-include required partition filters to query most recent partition.",
                    false,
                    true);

    public static final Param JOB_TIMEOUT =
            new Param(
                    "Query Job Timeout (seconds)",
                    Integer.class,
                    "BigQuery will attempt to cancel jobs that run longer (applicable to STANDARD_QUERY_API)",
                    false,
                    30);

    public static final Param[] parametersInfo = {
        PROJECT_ID,
        DATASET_NAME,
        SERVICE_ACCOUNT_KEY_FILE,
        ACCESS_METHOD,
        SIMPLIFY,
        USE_QUERY_CACHE,
        AUTO_ADD_PARTITION_FILTER,
        JOB_TIMEOUT
    };

    private static Pattern projectPattern = Pattern.compile("[a-zA-Z0-9_-]+");
    private static Pattern datasetPattern = Pattern.compile("[a-zA-Z0-9_]+");

    public BigqueryDataStoreFactory() {}

    @Override
    public String getDisplayName() {
        return "BigQuery Table";
    }

    @Override
    public String getDescription() {
        return "Read features from a Google BigQuery Geospatial Table, or View";
    }

    @Override
    public Param[] getParametersInfo() {
        return parametersInfo;
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            String projectId = (String) PROJECT_ID.lookUp(params);
            String datasetName = (String) DATASET_NAME.lookUp(params);
            BigqueryAccessMethod method = (BigqueryAccessMethod) ACCESS_METHOD.lookUp(params);
            Boolean simplify = (Boolean) SIMPLIFY.lookUp(params);
            Boolean useCache = (Boolean) USE_QUERY_CACHE.lookUp(params);

            boolean accessMethodValid =
                    ((method == BigqueryAccessMethod.STORAGE_API && !simplify)
                            || method == BigqueryAccessMethod.STANDARD_QUERY_API);
            boolean cacheValid =
                    ((method == BigqueryAccessMethod.STORAGE_API && !useCache)
                            || method == BigqueryAccessMethod.STANDARD_QUERY_API);
            boolean projectValid = projectPattern.matcher(projectId).matches();
            boolean datasetValid = datasetPattern.matcher(datasetName).matches();

            return cacheValid && accessMethodValid && projectValid && datasetValid;
        } catch (IOException e) {
            System.out.println(e);
            LOGGER.log(Level.WARNING, e.toString());
            return false;
        }
    }

    @Override
    public boolean isAvailable() {
        try {
            Class.forName("com.google.cloud.bigquery.BigQuery");
            Class.forName("com.google.cloud.bigquery.storage.v1.BigQueryReadClient");
            return true;
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "", e);
            return false;
        }
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        String serviceAccountKeyFileName = (String) params.get("Service Account Key File");

        try {
            File serviceAccountKeyFile = null;
            if (serviceAccountKeyFileName != null) {
                serviceAccountKeyFileName = serviceAccountKeyFileName.replace("file:///", "/");
                serviceAccountKeyFile = new File(serviceAccountKeyFileName);
            }

            return new BigqueryDataStore(
                    (String) PROJECT_ID.lookUp(params),
                    (String) DATASET_NAME.lookUp(params),
                    (BigqueryAccessMethod) ACCESS_METHOD.lookUp(params),
                    (Boolean) SIMPLIFY.lookUp(params),
                    (Boolean) USE_QUERY_CACHE.lookUp(params),
                    (Boolean) AUTO_ADD_PARTITION_FILTER.lookUp(params),
                    (Integer) JOB_TIMEOUT.lookUp(params),
                    serviceAccountKeyFile);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        return createDataStore(params);
    }
}
