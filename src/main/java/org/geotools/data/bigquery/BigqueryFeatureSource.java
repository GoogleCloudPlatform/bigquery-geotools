package org.geotools.data.bigquery;

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Read-only access to a Bigquery table
 *
 * @author traviswebb
 */
public class BigqueryFeatureSource extends ContentFeatureSource {

    private static final Logger LOGGER = Logging.getLogger(BigqueryFeatureSource.class);

    protected static final Map<StandardSQLTypeName, Class<?>> BQ_TYPE_MAP =
            new ImmutableMap.Builder<StandardSQLTypeName, Class<?>>()
                    .put(StandardSQLTypeName.GEOGRAPHY, Geometry.class)
                    .put(StandardSQLTypeName.BIGNUMERIC, BigDecimal.class)
                    .put(StandardSQLTypeName.BOOL, Boolean.class)
                    .put(StandardSQLTypeName.INT64, BigInteger.class)
                    .put(StandardSQLTypeName.FLOAT64, Float.class)
                    .put(StandardSQLTypeName.NUMERIC, BigDecimal.class)
                    .put(StandardSQLTypeName.STRING, String.class)
                    .put(StandardSQLTypeName.DATE, Date.class)
                    .put(StandardSQLTypeName.DATETIME, Date.class)
                    .put(StandardSQLTypeName.TIME, Date.class)
                    .put(StandardSQLTypeName.TIMESTAMP, Date.class)
                    .put(StandardSQLTypeName.BYTES, String.class)
                    .put(StandardSQLTypeName.ARRAY, String.class)
                    .put(StandardSQLTypeName.INTERVAL, String.class)
                    .put(StandardSQLTypeName.STRUCT, String.class)
                    .put(StandardSQLTypeName.JSON, String.class)
                    .build();

    public BigqueryFeatureSource(ContentEntry entry) throws IOException {
        super(entry, null);
    }

    private boolean getIsClustered() {
        BigqueryDataStore store = getDataStore();
        Table tableRef =
                store.bq.getTable(
                        TableId.of(store.projectId, store.datasetName, entry.getTypeName()));
        Clustering clustering =
                ((StandardTableDefinition) tableRef.getDefinition()).getClustering();

        if (clustering == null) {
            return false;
        }
        List<String> clusteringFields = clustering.getFields();
        return !clusteringFields.isEmpty()
                && getDataStore().GEOM_COLUMN.equals(clusteringFields.get(0));
    }

    @Override
    public BigqueryDataStore getDataStore() {
        return (BigqueryDataStore) super.getDataStore();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        BigqueryDataStore store = getDataStore();
        Table tableRef =
                store.bq.getTable(
                        TableId.of(store.projectId, store.datasetName, entry.getTypeName()));

        String sqlTable = tableRef.getGeneratedId().replace(":", ".");

        String geomColumn = getDataStore().GEOM_COLUMN;
        String sql = "SELECT ST_EXTENT(" + geomColumn + ") as extent FROM `" + sqlTable + "`";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();

        try {
            TableResult results = tableRef.getBigQuery().query(queryConfig);
            FieldValueList row = results.getValues().iterator().next();
            FieldValueList extent = row.get("extent").getRecordValue();

            return new ReferencedEnvelope(
                    extent.get("xmin").getDoubleValue(),
                    extent.get("xmax").getDoubleValue(),
                    extent.get("ymin").getDoubleValue(),
                    extent.get("ymax").getDoubleValue(),
                    DefaultGeographicCRS.WGS84);
        } catch (JobException e) {
            e.printStackTrace();
            return new ReferencedEnvelope();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ReferencedEnvelope();
        }
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        BigqueryDataStore store = getDataStore();
        Table tableRef =
                store.bq.getTable(
                        TableId.of(store.projectId, store.datasetName, entry.getTypeName()));
        return tableRef.getNumRows().intValue();
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new BigqueryFeatureReader(getState(), query);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        BigqueryDataStore store = getDataStore();
        Table tableRef =
                store.bq.getTable(
                        TableId.of(store.projectId, store.datasetName, entry.getTypeName()));

        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(entry.getTypeName());
        builder.setCRS(DefaultGeographicCRS.WGS84);

        Schema schema = tableRef.getDefinition().getSchema();
        FieldList fields = schema.getFields();
        for (Field field : fields) {
            String fieldName = field.getName();
            Class<?> fieldType = BQ_TYPE_MAP.get(field.getType().getStandardType());

            builder.add(fieldName, fieldType);
        }
        builder.setDefaultGeometry(getDataStore().GEOM_COLUMN);
        return builder.buildFeatureType();
    }
}
