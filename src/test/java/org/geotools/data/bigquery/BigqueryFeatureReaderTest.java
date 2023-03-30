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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQueryException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class BigqueryFeatureReaderTest {

    @Test
    public void testMaxFeatures() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.counties");
        q.setMaxFeatures(5);

        FeatureReader reader = store.getFeatureReader(q, Transaction.AUTO_COMMIT);

        for (int i = 0; i < 5; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = (SimpleFeature) reader.next();
            assertNotNull(f);
        }
        assertFalse(reader.hasNext());
    }

    @Test
    public void testSimpleViewQuery() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.counties_virginia_view");
        q.setMaxFeatures(3);

        FeatureReader reader = store.getFeatureReader(q, Transaction.AUTO_COMMIT);

        for (int i = 0; i < 3; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = (SimpleFeature) reader.next();
            assertNotNull(f);
        }
        assertFalse(reader.hasNext());
    }

    @Test
    public void testSimpleMaterializedViewQuery() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.counties_virginia_mview");
        q.setMaxFeatures(3);

        FeatureReader reader = store.getFeatureReader(q, Transaction.AUTO_COMMIT);

        for (int i = 0; i < 3; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = (SimpleFeature) reader.next();
            assertNotNull(f);
        }
        assertFalse(reader.hasNext());
    }

    @Test
    public void testDecorateQueryWithAutoPartitionFilter() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);
        params.put("Automatically query most recent Partition", true);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.port_traffic");

        store.getFeatureReader(q, Transaction.AUTO_COMMIT);
        // should not throw exception
    }

    @Test(expected = BigQueryException.class)
    public void testDecorateQueryWithManualPartitionFilter() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);
        params.put("Automatically query most recent Partition", false);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.port_traffic");
        store.getFeatureReader(q, Transaction.AUTO_COMMIT);
    }
}
