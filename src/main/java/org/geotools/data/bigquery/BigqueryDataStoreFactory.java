package org.geotools.data.bigquery;

import java.io.IOException;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

public class BigqueryDataStoreFactory implements DataStoreFactorySpi {

    @Override
    public String getDisplayName() {
        return "BigQuery";
    }

    @Override
    public String getDescription() {
        return "BigQuery Geospatial";
    }

    @Override
    public Param[] getParametersInfo() {
        return new Param[] {
            new Param("projectId", String.class, "Project ID", true, null),
            new Param("datasetName", String.class, "Dataset Name", true, null),
        };
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        return new BigqueryDataStore(
                (String) params.get("projectId"), (String) params.get("datasetName"));
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        return createDataStore(params);
    }
}
