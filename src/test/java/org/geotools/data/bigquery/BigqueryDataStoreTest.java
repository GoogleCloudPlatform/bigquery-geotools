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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryDataStoreTest {

    @Before
    public void cleanTables() {
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
        BigQuery queryClient = builder.setProjectId("bigquery-geotools").build().getService();

        String sql1 =
                "drop materialized view if exists `bigquery-geotools.test.counties_pregen_1m`";
        String sql2 =
                "drop materialized view if exists `bigquery-geotools.test.counties_pregen_10m`";
        String sql3 =
                "drop materialized view if exists `bigquery-geotools.test.counties_pregen_100m`";
        String sql4 =
                "drop materialized view if exists `bigquery-geotools.test.counties_pregen_1000m`";

        QueryJobConfiguration qConfig1 = QueryJobConfiguration.newBuilder(sql1).build();
        QueryJobConfiguration qConfig2 = QueryJobConfiguration.newBuilder(sql2).build();
        QueryJobConfiguration qConfig3 = QueryJobConfiguration.newBuilder(sql3).build();
        QueryJobConfiguration qConfig4 = QueryJobConfiguration.newBuilder(sql4).build();


        try {
            queryClient.query(qConfig1);
            queryClient.query(qConfig2);
            queryClient.query(qConfig3);
            queryClient.query(qConfig4);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetTypeLabel() {
        String tableName1 = BigqueryDataStore.getTypeLabel("bigquery-geotools.test.counties");
        String tableName2 =
                BigqueryDataStore.getTypeLabel("bigquery-geotools.test_dataset.counties_view");
        String tableName3 = BigqueryDataStore.getTypeLabel("bigquery-geotools.test.counties");

        assertEquals("bigquery-geotools.test.counties", tableName1);
        assertEquals("bigquery-geotools.test_dataset.counties_view", tableName2);
        assertEquals("bigquery-geotools.test.counties", tableName3);
    }

    @Test
    public void testCreateTypeNames() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        String[] names = store.getTypeNames();

        assertTrue(names.length > 0);
        assertEquals(names[0], "bigquery-geotools.test.counties");
    }

    @Test
    public void testCreateTypeNamesWithViews() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        List<String> names = Arrays.asList(store.getTypeNames());

        assertTrue(names.size() > 0);
        assertTrue(names.contains("bigquery-geotools.test.counties_virginia_mview"));
        assertTrue(names.contains("bigquery-geotools.test.counties_virginia_view"));
    }

    @Test
    public void testGetSchema() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureType type = store.getSchema("bigquery-geotools.test.counties");

        assertEquals(type.getTypeName(), "bigquery-geotools.test.counties");
        assertEquals(type.getAttributeCount(), 18);
    }

    @Test
    public void testGetFeatureSource() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureSource src = store.getFeatureSource("bigquery-geotools.test.counties");
        ReferencedEnvelope bbox = src.getBounds();

        assertEquals(144, bbox.getMinX(), 1);
        assertEquals(-14, bbox.getMinY(), 1);
        assertEquals(295, bbox.getMaxX(), 1);
        assertEquals(71, bbox.getMaxY(), 1);
    }

    @Test
    public void testGetFeatureReader() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);
        Query q = new Query("bigquery-geotools.test.counties");
        q.setMaxFeatures(10);

        FeatureReader reader = store.getFeatureReader(q, Transaction.AUTO_COMMIT);

        for (int i = 0; i < 10; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = (SimpleFeature) reader.next();
            assertTrue(f.getAttribute("county_fips_code") != null);

            Geometry geom = (Geometry) f.getAttribute("geom");
            assertEquals("Polygon", geom.getGeometryType());
            assertTrue(1 < geom.getLength());
            assertEquals(4326, geom.getSRID());
        }
    }

    @Test
    public void testPregenerateMaterializedViews() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);
        params.put("Pregenerate Materialized Views", BigqueryPregenerateOptions.MV_PREGEN_ALL);

        DataStore store = DataStoreFinder.getDataStore(params);
        ContentFeatureSource fs =
                (ContentFeatureSource) store.getFeatureSource("bigquery-geotools.test.counties");

        // invokes buildFeatureType()
        fs.getSchema();
        
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
        BigQuery queryClient = builder.setProjectId("bigquery-geotools").build().getService();

        String sql = "select * from `bigquery-geotools.test.INFORMATION_SCHEMA.MATERIALIZED_VIEWS`";
        QueryJobConfiguration qc = QueryJobConfiguration.newBuilder(sql).build();

        try {
            TableResult results = queryClient.query(qc);
            List<String> foundViews = new ArrayList<String>();

            for (FieldValueList row : results.iterateAll()) {
                String viewName = row.get("table_name").getStringValue();
                if (viewName.indexOf("_pregen_") != -1) {
                    foundViews.add(viewName);
                }
            }

            assertEquals(4, foundViews.size());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
