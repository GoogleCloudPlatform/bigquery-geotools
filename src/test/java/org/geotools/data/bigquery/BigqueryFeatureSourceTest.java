package org.geotools.data.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;

public class BigqueryFeatureSourceTest {

    @Test
    public void testGetFeatures() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        params.put("geometryColumn", "geom");

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("counties");
        q.setMaxFeatures(100);

        SimpleFeatureSource fs = (SimpleFeatureSource) store.getFeatureSource("counties");
        SimpleFeatureCollection col = fs.getFeatures(q);

        assertEquals(100, col.size());

        SimpleFeature feature = col.features().next();
        Geometry geom = (Geometry) feature.getDefaultGeometry();

        assertTrue(geom.getArea() > 0.1);
        assertTrue(geom.getLength() > 0.1);
        assertEquals("Polygon", geom.getGeometryType());
        assertEquals(4326, geom.getSRID());
        assertTrue(geom.isValid());
    }
}
