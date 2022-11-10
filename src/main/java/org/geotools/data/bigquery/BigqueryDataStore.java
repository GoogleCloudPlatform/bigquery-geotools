package org.geotools.data.bigquery;

import com.google.api.gax.paging.Page;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;

/**
 * Geotools datastore for BigQuery
 *
 * @author traviswebb
 */
public class BigqueryDataStore extends ContentDataStore {

    private BigQuery bq;

    private String projectId;
    private String datasetName;

    /**
     * Construct a BigqueryDatastore for a given project and dataset.
     *
     * @param projectId
     * @param datasetName
     */
    public BigqueryDataStore(String projectId, String datasetName) {
        this.projectId = projectId;
        this.datasetName = datasetName;

        this.setNamespaceURI(null);

        this.bq = BigQueryOptions.newBuilder().setProjectId(this.projectId).build().getService();
    }

    @Override
    /** Return list of BQ tables in the given dataset that contain a GEOGRAPHY column. */
    protected List<Name> createTypeNames() throws IOException {
        List<Name> typeNames = new ArrayList<>();
        try {
            Page<Table> tables = bq.listTables(this.datasetName, TableListOption.pageSize(100));
            for (Table table : tables.iterateAll()) {
                if (null != getTableGeometryColumn(table)) {
                    typeNames.add(new NameImpl(getTableName(table.getGeneratedId())));
                }
            }
        } catch (BigQueryException e) {
            throw new IOException(e);
        }

        return typeNames;
    }

    public static String getTableName(String tableName) {
        // System.out.println(table.getGeneratedId());
        String[] parts = tableName.split("\\.");
        return parts[parts.length - 1];
    }

    protected String getTableGeometryColumn(Table table) {
        Table hydratedTable = bq.getTable(table.getTableId());
        Schema schema = hydratedTable.getDefinition().getSchema();
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
    /** Get BigqueryFeatureSource */
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        TableId tableId = TableId.of(projectId, datasetName, entry.getTypeName());
        Table table = bq.getTable(tableId);
        return new BigqueryFeatureSource(entry, null, table);
    }

    BigQuery read() throws IOException {
        return bq;
    }
}
