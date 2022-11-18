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
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

public class BigqueryFeatureReaderTest {
    @Test
    public void testFeatureReaderBasic() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureReader reader =
                (SimpleFeatureReader)
                        store.getFeatureReader(new Query("counties"), Transaction.AUTO_COMMIT);

        assertTrue(reader.hasNext());

        SimpleFeature f = reader.next();
        assertTrue(f.getAttribute("county_fips_code") != null);

        Geometry geom = (Geometry) f.getAttribute("geom");
        assertEquals("Polygon", geom.getGeometryType());
        assertTrue(1 < geom.getLength());
        assertEquals(4326, geom.getSRID());
    }
}
