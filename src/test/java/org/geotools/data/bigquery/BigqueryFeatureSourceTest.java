package org.geotools.data.bigquery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.junit.Test;

public class BigqueryFeatureSourceTest {

    @Test
    public void testGetFeatures() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureSource fs = (SimpleFeatureSource) store.getFeatureSource("counties");

        SimpleFeatureCollection col = fs.getFeatures();

        // System.out.println(col.size());
    }
}
