package org.geotools.data.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.logging.Logging;

public class BigqueryDataStoreFactory implements DataStoreFactorySpi {
    private static final Logger LOGGER = Logging.getLogger(BigqueryDataStoreFactory.class);

    public static final Param PROJECT_ID =
            new Param("projectId", String.class, "GCP Project ID", true, null);

    public static final Param DATASET_NAME =
            new Param("datasetName", String.class, "BigQuery Dataset Name", true, null);

    public static final Param SERVICE_ACCOUNT_KEY_FILE =
            new Param("serviceAccountKeyFile", File.class, "Service Account Key File", false, null);

    public static final Param GEOM_COLUMN =
            new Param("geometryColumn", String.class, "Geometry Column Name", false, "geom");

    public static final Param[] parametersInfo = {
        PROJECT_ID, DATASET_NAME, SERVICE_ACCOUNT_KEY_FILE, GEOM_COLUMN
    };

    private static Pattern projectPattern = Pattern.compile("[a-zA-Z0-9_-]+");
    private static Pattern datasetPattern = Pattern.compile("[a-zA-Z0-9_]+");

    public BigqueryDataStoreFactory() {}

    @Override
    public String getDisplayName() {
        return "Google BigQuery";
    }

    @Override
    public String getDescription() {
        return "Google BigQuery Dataset";
    }

    @Override
    public Param[] getParametersInfo() {
        return parametersInfo;
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        boolean containsRequired =
                params.containsKey("projectId") && params.containsKey("datasetName");

        if (!containsRequired) return false;

        String projectId = (String) params.get("projectId");
        String datasetName = (String) params.get("datasetName");

        boolean projectValid = projectPattern.matcher(projectId).matches();
        boolean datasetValid = datasetPattern.matcher(datasetName).matches();

        return projectValid && datasetValid;
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
        try {
            return new BigqueryDataStore(
                    (String) params.get("projectId"),
                    (String) params.get("datasetName"),
                    (String) params.get("geometryColumn"),
                    (File) params.get("serviceAccountKeyFile"));
        } catch (BigQueryException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        return createDataStore(params);
    }
}
