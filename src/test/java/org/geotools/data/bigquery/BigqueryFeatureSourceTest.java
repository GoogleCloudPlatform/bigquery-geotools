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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentFeatureSource;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class BigqueryFeatureSourceTest {

    @Test
    public void testGetFeatures() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        Query q = new Query("bigquery-geotools.test.counties");
        q.setMaxFeatures(100);

        SimpleFeatureSource fs =
                (SimpleFeatureSource) store.getFeatureSource("bigquery-geotools.test.counties");
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

    @Test
    public void testBuildFeatureTypeBasic() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        ContentFeatureSource fs =
                (ContentFeatureSource) store.getFeatureSource("bigquery-geotools.test.counties");

        SimpleFeatureType featureType = fs.getSchema();
        assertNotNull(featureType);
    }

    @Test
    public void testBuildFeatureTypeView() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        ContentFeatureSource fs =
                (ContentFeatureSource)
                        store.getFeatureSource("bigquery-geotools.test.counties_virginia_view");

        SimpleFeatureType featureType = fs.getSchema();
        assertNotNull(featureType);
    }

    @Test
    public void testBuildFeatureTypeMaterializedView() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        ContentFeatureSource fs =
                (ContentFeatureSource)
                        store.getFeatureSource("bigquery-geotools.test.counties_virginia_mview");

        SimpleFeatureType featureType = fs.getSchema();
        assertNotNull(featureType);
    }

    @Test
    public void testBuildFeatureTypeOSM() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        ContentFeatureSource fs =
                (ContentFeatureSource)
                        store.getFeatureSource("bigquery-geotools.test.planet_nodes");

        SimpleFeatureType featureType = fs.getSchema();
        assertNotNull(featureType);

        AttributeDescriptor geomAttr = featureType.getDescriptor("geometry");
        assertTrue((Boolean) geomAttr.getUserData().get("clustering"));
    }

    @Test
    public void testBuildFeatureTypePartitioned() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("Project Id", "bigquery-geotools");
        params.put("Dataset Name", "test");
        params.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        DataStore store = DataStoreFinder.getDataStore(params);

        ContentFeatureSource fs =
                (ContentFeatureSource)
                        store.getFeatureSource("bigquery-geotools.test.port_traffic");

        SimpleFeatureType featureType = fs.getSchema();
        assertNotNull(featureType);

        AttributeDescriptor geomAttr = featureType.getDescriptor("port_geom");
        assertFalse((Boolean) geomAttr.getUserData().get("clustering"));
        assertFalse((Boolean) geomAttr.getUserData().get("partitioning"));
        assertFalse((Boolean) geomAttr.getUserData().get("partitioningRequired"));

        AttributeDescriptor partAttr = featureType.getDescriptor("week_end");
        assertTrue((Boolean) partAttr.getUserData().get("partitioning"));
        assertTrue((Boolean) partAttr.getUserData().get("partitioningRequired"));
        assertEquals("DAY", partAttr.getUserData().get("partitioningType"));
    }
}
