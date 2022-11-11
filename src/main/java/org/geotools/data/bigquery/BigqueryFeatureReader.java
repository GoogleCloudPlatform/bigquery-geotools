package org.geotools.data.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryFeatureReader implements SimpleFeatureReader {

    protected ContentState state;
    protected Table table;
    protected Schema schema;
    protected FieldList fields;
    protected Iterator<FieldValueList> cursor;
    protected GeometryFactory gf;

    public BigqueryFeatureReader(ContentState state, Table table, Query query) {
        this.table = table;
        this.schema = table.getDefinition().getSchema();
        this.fields = schema.getFields();

        Page<FieldValueList> page = table.list(schema, TableDataListOption.pageSize(100));

        this.cursor = page.iterateAll().iterator();
        this.state = state;
        this.gf = JTSFactoryFinder.getGeometryFactory(null);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {

        return readFeature(cursor.next());
    }

    public SimpleFeature readFeature(FieldValueList row) throws IOException {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(state.getFeatureType());

        for (Field field : fields) {
            String column = field.getName();
            if (column == "geom") continue;

            builder.set(column, row.get(column).getValue());
        }

        String geomWkt = row.get(BigqueryDataStore.GEOM_COLUMN).getStringValue();

        try {
            Geometry geom = new WKTReader().read(geomWkt);
            geom.setSRID(BigqueryDataStore.SRID);
            builder.set(BigqueryDataStore.GEOM_COLUMN, geom);
        } catch (ParseException e) {
            throw new IOException(e);
        }
        return builder.buildFeature(UUID.randomUUID().toString());
    }

    @Override
    public boolean hasNext() throws IOException {
        return cursor.hasNext();
    }

    @Override
    public void close() throws IOException {}
}
