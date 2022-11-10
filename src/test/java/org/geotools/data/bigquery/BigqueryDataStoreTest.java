package org.geotools.data.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryDataStoreTest {

    @Test
    public void testGetTableName() {
        String tableName = BigqueryDataStore.getTableName("bigquery-geotools:test.counties");
        assertEquals(tableName, "counties");
    }

    @Test
    public void testCreateTypeNames() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        String[] names = store.getTypeNames();

        assertTrue(names.length > 0);
        assertEquals(names[0], "counties");
    }

    @Test
    public void testGetSchema() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureType type = store.getSchema("counties");

        assertEquals(type.getTypeName(), "counties");
        assertEquals(type.getAttributeCount(), 18);
    }

    @Test
    public void testGetFeatureSource() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureSource src = store.getFeatureSource("counties");
        ReferencedEnvelope bbox = src.getBounds();

        assertEquals(144, bbox.getMinX(), 1);
        assertEquals(-14, bbox.getMinY(), 1);
        assertEquals(295, bbox.getMaxX(), 1);
        assertEquals(71, bbox.getMaxY(), 1);
    }

    @Test
    public void testGetFeatureReader() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureReader reader =
                (SimpleFeatureReader)
                        store.getFeatureReader(new Query("counties"), Transaction.AUTO_COMMIT);

        for (int i = 0; i < 10; i++) {
            assertTrue(reader.hasNext());

            SimpleFeature f = reader.next();
            assertTrue(f.getAttribute("county_fips_code") != null);

            Geometry geom = (Geometry) f.getAttribute("geom");
            assertEquals("Polygon", geom.getGeometryType());
            assertTrue(1 < geom.getLength());
            assertEquals(4326, geom.getSRID());
            // System.out.println(i + " " + geom);

            // assertTrue()
        }
    }
}
