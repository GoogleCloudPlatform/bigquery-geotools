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

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

            builder.set(column, row.get(column).getValue());
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
