package org.geotools.data.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Read-only access to a Bigquery table
 *
 * @author traviswebb
 */
public class BigqueryFeatureSource extends ContentFeatureSource {

    private Table table;
    private final String sqlTable;

    protected final String GEOM_COLUMN = "geom";

    public BigqueryFeatureSource(ContentEntry entry, Query query, Table table) {
        super(entry, query);
        this.table = table;
        this.sqlTable = table.getGeneratedId().replace(":", ".");
        // System.out.println("BigqueryFeatureSource()");
    }

    @Override
    public BigqueryDataStore getDataStore() {
        return (BigqueryDataStore) super.getDataStore();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        BigQuery bq = table.getBigQuery();

        String sql = "SELECT ST_EXTENT(geom) as extent FROM `" + sqlTable + "`";
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
        try {
            TableResult results = bq.query(queryConfig);
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
        return table.getNumRows().intValue();
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
            throws IOException {
        return new BigqueryFeatureReader(getState(), table, query);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName(entry.getTypeName());
        builder.setCRS(DefaultGeographicCRS.WGS84);

        Schema schema = table.getDefinition().getSchema();
        FieldList fields = schema.getFields();
        for (Field field : fields) {
            StandardSQLTypeName fieldType = field.getType().getStandardType();
            String fieldName = field.getName();

            if (fieldType == StandardSQLTypeName.GEOGRAPHY) {
                builder.add(fieldName, Geometry.class);
            } else if (fieldType == StandardSQLTypeName.BIGNUMERIC) {
                builder.add(fieldName, BigDecimal.class);
            } else if (fieldType == StandardSQLTypeName.BOOL) {
                builder.add(fieldName, Boolean.class);
            } else if (fieldType == StandardSQLTypeName.INT64) {
                builder.add(fieldName, BigInteger.class);
            } else if (fieldType == StandardSQLTypeName.FLOAT64) {
                builder.add(fieldName, Float.class);
            } else if (fieldType == StandardSQLTypeName.NUMERIC) {
                builder.add(fieldName, BigDecimal.class);
            } else if (fieldType == StandardSQLTypeName.STRING) {
                builder.add(fieldName, String.class);
            } else if (fieldType == StandardSQLTypeName.DATE
                    || fieldType == StandardSQLTypeName.DATETIME) {
                builder.add(fieldName, Date.class);
            } else if (fieldType == StandardSQLTypeName.TIME
                    || fieldType == StandardSQLTypeName.TIMESTAMP) {
                builder.add(fieldName, Date.class);
            }
            // skipping BYTES, ARRAY, INTERVAL, JSON, STRUCT
        }
        builder.setDefaultGeometry(BigqueryDataStore.GEOM_COLUMN);
        return builder.buildFeatureType();
    }
}
