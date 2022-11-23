package org.geotools.data.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.spatial.BBOX;

public class BigqueryFeatureReaderTest {
    @Test
    public void testFeatureReaderBasic() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        params.put("geometryColumn", "geom");

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

    @Test
    /**
     * Query: feature type: counties filter: [ FastBBOX [ property=geom,
     * envelope=ReferencedEnvelope[ 281.3214081689755 : 285.5841094091495, 36.00496223129481 :
     * 38.44938347183019]] OR FastBBOX [ property=geom, envelope=ReferencedEnvelope[
     * -78.6785918310245 : -74.4158905908505, 36.00496223129481 : 38.44938347183019]] ] [properties:
     * geom]
     *
     * @throws IOException
     */
    public void testSingleBboxFilter() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        params.put("geometryColumn", "geom");

        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureReader reader =
                (SimpleFeatureReader)
                        store.getFeatureReader(new Query("counties"), Transaction.AUTO_COMMIT);

        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);
        BBOX bbox = ff.bbox("geom", -78.6785, 36.0049, -74.4158, 38.4493, "epsg:4326");
        Query q = new Query("counties", bbox);

        BigqueryQueryParser parser = new BigqueryQueryParser(q, reader.getFeatureType());
        TableReadOptions options = parser.parse().toReadOptions();

        assertEquals(
                "ST_INTERSECTSBOX(geom, -78.678500, 36.004900, -74.415800, 38.449300)",
                options.getRowRestriction());
    }

    /*
     * OR [FastBBOX [property=geom, envelope=ReferencedEnvelope[
     * 281.3214081689755 : 285.5841094091495, 36.00496223129481 : 38.44938347183019]]
     */
    @Test
    public void testCompoundBboxFilter() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("projectId", "bigquery-geotools");
        params.put("datasetName", "test");
        params.put("geometryColumn", "geom");

        DataStore store = DataStoreFinder.getDataStore(params);

        SimpleFeatureReader reader =
                (SimpleFeatureReader)
                        store.getFeatureReader(new Query("counties"), Transaction.AUTO_COMMIT);

        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

        BBOX bbox1 = ff.bbox("geom", -281.321, 36.0049, 285.584, 38.4493, "epsg:4326");
        BBOX bbox2 = ff.bbox("geom", -78.6785, 36.0049, -74.4158, 38.4493, "epsg:4326");

        Filter orFilter = ff.or(bbox1, bbox2);

        Query q = new Query("counties", orFilter);

        BigqueryQueryParser parser = new BigqueryQueryParser(q, reader.getFeatureType());
        TableReadOptions options = parser.parse().toReadOptions();

        assertEquals(
                "ST_INTERSECTSBOX(geom, -180.000000, 36.004900, 180.000000, 38.449300) OR ST_INTERSECTSBOX(geom, -78.678500, 36.004900, -74.415800, 38.449300)",
                options.getRowRestriction());

        // (lng1:83.671685, lat1:-49.744917, lng2:356.484565, lat2:106.698042)
    }
}
