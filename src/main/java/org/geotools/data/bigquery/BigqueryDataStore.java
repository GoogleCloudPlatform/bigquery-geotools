package org.geotools.data.bigquery;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.util.logging.Logging;
import org.opengis.feature.type.Name;

/** Geotools datastore for BigQuery */
public class BigqueryDataStore extends ContentDataStore {

    private static final Logger LOGGER = Logging.getLogger(BigqueryDataStore.class);

    /** Parameterize this if/when BQ supports non-WGS84 SRIDs. Constant for now. */
    protected final int SRID = 4326;

    protected final String GEOM_COLUMN;

    protected final BigQuery bq;
    protected final String projectId;
    protected final String datasetName;
    protected final File serviceAccountKeyFile;
    protected GoogleCredentials credentials;

    /**
     * Construct a BigqueryDatastore for a given project and dataset.
     *
     * @param projectId
     * @param datasetName
     */
    public BigqueryDataStore(
            String projectId, String datasetName, String geomColumn, File serviceAccountKeyFile)
            throws IOException {
        this.GEOM_COLUMN = geomColumn;
        this.projectId = projectId;
        this.datasetName = datasetName;
        this.serviceAccountKeyFile = serviceAccountKeyFile;

        this.setNamespaceURI(null);

        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
        if (serviceAccountKeyFile != null) {
            try (FileInputStream serviceAccountStream =
                    new FileInputStream(serviceAccountKeyFile)) {
                credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            }
            builder.setCredentials(credentials);
        }

        this.bq = builder.setProjectId(projectId).build().getService();
    }

    @Override
    /** Return list of BQ tables in the given dataset that contain a GEOGRAPHY column. */
    protected List<Name> createTypeNames() throws IOException {
        List<Name> typeNames = new ArrayList<>();
        try {
            Page<Table> tables = bq.listTables(this.datasetName, TableListOption.pageSize(100));
            for (Table table : tables.iterateAll()) {
                Table hydratedTable = bq.getTable(table.getTableId());
                Schema schema = hydratedTable.getDefinition().getSchema();
                if (null != getTableGeometryColumn(schema)) {
                    typeNames.add(new NameImpl(getTableName(table.getGeneratedId())));
                }
            }
        } catch (BigQueryException e) {
            throw new IOException(e);
        }

        return typeNames;
    }

    public static String getTableName(String tableName) {
        String[] parts = tableName.split("\\.");
        return parts[parts.length - 1];
    }

    protected String getTableGeometryColumn(Schema schema) {
        FieldList fields = schema.getFields();
        for (Field field : fields) {
            StandardSQLTypeName fieldType = field.getType().getStandardType();
            if (fieldType == StandardSQLTypeName.GEOGRAPHY) {
                return field.getName();
            }
        }

        return null;
    }

    @Override
    /** Return a new BigqueryFeatureSource */
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        String projectUri = String.format("projects/%s", projectId);
        String tableUri =
                String.format(
                        "projects/%s/datasets/%s/tables/%s",
                        projectId, datasetName, entry.getTypeName());

        Table tableRef = bq.getTable(TableId.of(projectId, datasetName, entry.getTypeName()));

        return new BigqueryFeatureSource(entry, tableRef, projectUri, tableUri);
    }

    BigQuery read() throws IOException {
        return bq;
    }
}
