/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.geotools.data.bigquery;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.geotools.data.Query;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.AttributeDescriptor;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;

@SuppressWarnings("deprecation")
public class BigqueryStandardReader extends BigqueryFeatureReader {

    private static final Logger LOGGER = Logging.getLogger(BigqueryStandardReader.class);

    private Iterator<FieldValueList> cursor;

    public BigqueryStandardReader(ContentState state, Query query) throws IOException {
        super(state, query);

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(getSQLFromGeotoolsQuery()).build();

        try {
            cursor = store.queryClient.query(queryConfig).iterateAll().iterator();
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Return SQL from the given Query.
     *
     * @return
     */
    protected String getSQLFromGeotoolsQuery() {
        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(query, getFeatureType(), store.CRS, store.pregen);

        String sql =
                String.format(
                        "SELECT %s FROM `%s` WHERE %s LIMIT %d",
                        parser.getSelectClause(store.simplify),
                        query.getTypeName(),
                        parser.getWhereClause(),
                        rowLimit);

        System.out.println(sql);

        return sql;
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {

        rowIndex++;
        return parseFeature(cursor.next());
    }

    @Override
    public boolean hasNext() throws IOException {
        return cursor.hasNext();
    }

    /**
     * Tries to find a matching getter, using Reflection,
     * for the AttributeDescriptor and returns the value.
     * @param 
     * @return
     * @throws IOException
     */
    protected class AttributeValueGetter{

        private final AttributeDescriptor attr;
        private final FieldValueList row;

        public AttributeValueGetter(AttributeDescriptor attr, FieldValueList row) {
            this.attr = attr;
            this.row = row;
        }

        public Method getValueGetter() throws NoSuchMethodException  {
            String column = attr.getLocalName();
            FieldValue value = row.get(column);

            if (value.isNull()) {
                return null;
            }

            // Using reflection, try to find if there is a getter for that type in the
            // type in the FieldValue class.
            try {
                
                String typeName = attr.getType().getBinding().getSimpleName();
                return value.getClass().getMethod("get" + typeName + "Value");

            } catch (Exception e) {
                return value.getClass().getMethod("getValue");
            }
        }
    }


    protected SimpleFeature parseFeature(FieldValueList row) throws IOException {
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);

        List<String> returnedColumns = new ArrayList<String>();
        if (!query.retrieveAllProperties()) {
            returnedColumns.addAll(Arrays.asList(query.getPropertyNames()));
        }
        for (AttributeDescriptor attr : featureType.getAttributeDescriptors()) {
            String column = attr.getLocalName();

            if (!query.retrieveAllProperties() && !returnedColumns.contains(column)) continue;
            if (column == geomColumn) continue;

            try {
                AttributeValueGetter getter = new AttributeValueGetter(attr, row);
                Method getterMethod = getter.getValueGetter();
                if (getterMethod == null) continue;

                // If this is a timestamp - it returns a long, but we want an Instant.
                if (getterMethod.getName().equals("getTimestampValue")) {
                    Object value = getterMethod.invoke(row.get(column));
                    String iso8601Date = Instant.ofEpochMilli((long) value/1000).toString();
                    builder.set(column, iso8601Date);
                    continue;
                }
                Object value = getterMethod.invoke(row.get(column));
                builder.set(column, value);
            } catch (Exception e) {
                builder.set(column, row.get(column).getValue());
            }
        }

        try {
            String geomGeojson = row.get(geomColumn).getStringValue();
            InputStream stream =
                    new ByteArrayInputStream(geomGeojson.getBytes(StandardCharsets.UTF_8));
            Geometry geom = new GeometryJSON().read(stream);

            geom.setSRID(srid);
            builder.set(geomColumn, geom);
        } catch (Exception e) {
            System.out.println(e);
        }

        return builder.buildFeature(Integer.toString(rowIndex));
    }
}
