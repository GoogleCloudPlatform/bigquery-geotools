package org.geotools.data.bigquery;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery.TableDataListOption;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Table;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

    protected ContentState state;
    protected Table table;
    protected Iterator<FieldValueList> cursor;
    protected SimpleFeatureBuilder builder;
    protected GeometryFactory gf;

    public BigqueryFeatureReader(ContentState state, Table table, Query query) {
        Page<FieldValueList> page =
                table.list(table.getDefinition().getSchema(), TableDataListOption.pageSize(100));

        this.table = table;
        this.cursor = page.iterateAll().iterator();
        this.state = state;
        this.builder = new SimpleFeatureBuilder(state.getFeatureType());
        this.gf = JTSFactoryFinder.getGeometryFactory(null);

        System.out.println("BigqueryFeatureReader()");
        System.out.println("table " + table);
        System.out.println("state " + state);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return state.getFeatureType();
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {
        ArrayList<String> keys = new ArrayList<>();
        FieldValueList row = cursor.next();

        for (Field s : table.getDefinition().getSchema().getFields()) {
            keys.add(s.getName());
        }
        for (String column : keys) {
            builder.set(column, row.get(column));
        }
        SimpleFeature f = builder.buildFeature(UUID.randomUUID().toString());
        System.out.println(f);

        return f;
    }

    @Override
    public boolean hasNext() throws IOException {
        return cursor.hasNext();
    }

    @Override
    public void close() throws IOException {}
}
