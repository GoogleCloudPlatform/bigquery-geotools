package org.geotools.data.bigquery;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        params.put("geometryColumn", "geom");

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("counties");
        q.setMaxFeatures(5);

        FeatureReader reader = store.getFeatureReader(q, Transaction.AUTO_COMMIT);

        for (int i = 0; i < 5; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = (SimpleFeature) reader.next();
            assertNotNull(f);
        }
        assertFalse(reader.hasNext());
    }
}
