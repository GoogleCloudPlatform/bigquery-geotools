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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryDataStoreTest {

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

        String[] names = store.getTypeNames();

        assertTrue(names.length > 0);
        assertEquals(names[1], "bigquery-geotools.test.counties_virginia_mview");
        assertEquals(names[2], "bigquery-geotools.test.counties_virginia_view");
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
}
