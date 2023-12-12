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

import java.util.Date;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.iso.PositionFactoryImpl;
import org.geotools.geometry.iso.PrecisionModel;
import org.geotools.geometry.iso.aggregate.AggregateFactoryImpl;
import org.geotools.geometry.iso.complex.ComplexFactoryImpl;
import org.geotools.geometry.iso.coordinate.GeometryFactoryImpl;
import org.geotools.geometry.iso.primitive.PrimitiveFactoryImpl;
import org.geotools.geometry.iso.text.WKTParser;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Not;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.spatial.BBOX;
import org.opengis.geometry.PositionFactory;
import org.opengis.geometry.Precision;
import org.opengis.geometry.aggregate.AggregateFactory;
import org.opengis.geometry.coordinate.GeometryFactory;
import org.opengis.geometry.primitive.PrimitiveFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.picocontainer.DefaultPicoContainer;

// import org.geotools.geometry.jts.spatialschema.PositionFactoryImpl;

public class BigqueryFilterVisitorTest {

    SimpleFeatureType countiesFeatureType;
    FilterFactory2 ff;
    WKTParser wktParser;
    CoordinateReferenceSystem CRS = DefaultGeographicCRS.WGS84;
    BigqueryPregenerateOptions pregenNone = BigqueryPregenerateOptions.MV_NONE;

    @Before
    public void setupFeatureType() {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("counties");
        builder.setCRS(DefaultGeographicCRS.WGS84);
        builder.add("geom", org.opengis.geometry.Geometry.class);
        builder.add("name", String.class);
        builder.add("population", Integer.class);
        builder.add("date", Date.class);
        builder.setDefaultGeometry("geom");
        countiesFeatureType = builder.buildFeatureType();
    }

    @Before
    public void setupFactories() throws FactoryException {
        ff = CommonFactoryFinder.getFilterFactory2(null);

        DefaultPicoContainer container = new DefaultPicoContainer();
        container.addComponent(PositionFactoryImpl.class);
        container.addComponent(AggregateFactoryImpl.class);
        container.addComponent(ComplexFactoryImpl.class);
        container.addComponent(GeometryFactoryImpl.class);
        container.addComponent(PrimitiveFactoryImpl.class);

        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;
        container.addComponent(crs);
        Precision pr = new PrecisionModel();
        container.addComponent(pr);

        PositionFactoryImpl pf =
                (PositionFactoryImpl) container.getComponent(PositionFactory.class);
        GeometryFactoryImpl gf =
                (GeometryFactoryImpl) container.getComponent(GeometryFactory.class);
        PrimitiveFactoryImpl prf =
                (PrimitiveFactoryImpl) container.getComponent(PrimitiveFactory.class);
        AggregateFactoryImpl af =
                (AggregateFactoryImpl) container.getComponent(AggregateFactory.class);

        wktParser = new WKTParser(gf, prf, pf, af);
    }

    @Test
    public void testSpatialBBOX() {
        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

        BBOX bbox1 = ff.bbox("geom", -78.6785, 36.0049, -74.4158, 38.4493, "epsg:4326");

        Query q = new Query("counties", bbox1);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_INTERSECTSBOX(geom, -78.678500, 36.004900, -74.415800, 38.449300)",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialIntersectsPointFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter intersectsFilter = ff.intersects(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", intersectsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialIntersectsOrFilter() throws ParseException {
        String wktPoint1 = "POINT(-76.3 36.8)";
        // String wktPoint2 = "POINT(-76.4 36.9)";

        Geometry geom1 = new WKTReader().read(wktPoint1);
        Geometry geom2 = new WKTReader().read(wktPoint1);

        Filter intersectsFilter1 = ff.intersects(ff.property("geom"), ff.literal(geom1));
        Filter intersectsFilter2 = ff.intersects(ff.property("geom"), ff.literal(geom2));
        Filter orFilter = ff.or(intersectsFilter1, intersectsFilter2);

        Query q = new Query("counties", orFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.3,36.8]}', make_valid => true)) OR ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.3,36.8]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialIntersectsAndFilter() throws ParseException {
        String wktPoint1 = "POINT(-76.3 36.8)";
        // String wktPoint2 = "POINT(-76.4 36.9)";

        Geometry geom1 = new WKTReader().read(wktPoint1);
        Geometry geom2 = new WKTReader().read(wktPoint1);

        Filter intersectsFilter1 = ff.intersects(ff.property("geom"), ff.literal(geom1));
        Filter intersectsFilter2 = ff.intersects(ff.property("geom"), ff.literal(geom2));
        Filter andFilter = ff.and(intersectsFilter1, intersectsFilter2);

        Query q = new Query("counties", andFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.3,36.8]}', make_valid => true)) AND ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.3,36.8]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialIntersectsPolygonFilter() throws ParseException {
        String wktPolygon = "POLYGON((-76.2 36.8, -76.1 36.8, -76.1 36.7, -76.3 36.6, -76.2 36.8))";
        Geometry geom = new WKTReader().read(wktPolygon);
        Filter intersectsFilter = ff.intersects(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", intersectsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_INTERSECTS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Polygon\",\"coordinates\":[[[-76.2,36.8],[-76.1,36.8],[-76.1,36.7],[-76.3,36.6],[-76.2,36.8]]]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialDWithinFilter() throws ParseException {
        double distance = 123.45;
        String units = "meters";
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter dwithinFilter = ff.dwithin(ff.property("geom"), ff.literal(geom), distance, units);
        Query q = new Query("counties", dwithinFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_DWITHIN(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true), 123.450000)",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialContainsFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter containsFilter = ff.contains(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", containsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_CONTAINS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialContainsReverseArgFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter containsFilter = ff.contains(ff.literal(geom), ff.property("geom"));
        Query q = new Query("counties", containsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_CONTAINS(ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true), geom)",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialDisjointFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter disjointFilter = ff.disjoint(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", disjointFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_DISJOINT(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialTouchesFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter touchesFilter = ff.touches(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", touchesFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_TOUCHES(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialEqualsFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter equalsFilter = ff.equal(ff.property("geom"), ff.literal(geom));
        Query q = new Query("counties", equalsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_EQUALS(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true))",
                parser.getWhereClause());
    }

    @Test
    public void testEqualsReverseArgFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        Geometry geom = new WKTReader().read(wktPoint);
        Filter equalsFilter = ff.equal(ff.literal(geom), ff.property("geom"));
        Query q = new Query("counties", equalsFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_EQUALS(ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true), geom)",
                parser.getWhereClause());
    }

    @Test
    public void testSpatialWithinFilter() throws ParseException {
        String wktPoint = "POINT(-76.2859 36.8508)";
        String wktPolygon = "POLYGON((-76.2 36.8, -76.1 36.8, -76.1 36.7, -76.3 36.6, -76.2 36.8))";
        Geometry geomPoint = new WKTReader().read(wktPoint);
        Geometry geomPoly = new WKTReader().read(wktPolygon);

        Filter withinFilter1 = ff.within(ff.property("geom"), ff.literal(geomPoly));
        Filter withinFilter2 = ff.within(ff.literal(geomPoint), ff.property("geom"));
        Filter withinFilter3 = ff.within(ff.property("geom1"), ff.property("geom2"));

        Query q1 = new Query("counties", withinFilter1);
        Query q2 = new Query("counties", withinFilter2);
        Query q3 = new Query("counties", withinFilter3);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q1, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_WITHIN(geom, ST_GEOGFROMGEOJSON('{\"type\":\"Polygon\",\"coordinates\":[[[-76.2,36.8],[-76.1,36.8],[-76.1,36.7],[-76.3,36.6],[-76.2,36.8]]]}', make_valid => true))",
                parser.getWhereClause());

        parser = new BigqueryFilterVisitor(q2, countiesFeatureType, CRS, pregenNone);

        assertEquals(
                "ST_WITHIN(ST_GEOGFROMGEOJSON('{\"type\":\"Point\",\"coordinates\":[-76.2859,36.8508]}', make_valid => true), geom)",
                parser.getWhereClause());

        parser = new BigqueryFilterVisitor(q3, countiesFeatureType, CRS, pregenNone);

        assertEquals("ST_WITHIN(geom2, geom1)", parser.getWhereClause());
    }

    // NON-SPATIAL TESTS

    @Test
    public void testPropertyEqualFilter() throws ParseException {
        PropertyIsEqualTo stringEqualFilter =
                ff.equals(ff.property("name"), ff.property("population"));
        Query q = new Query("counties", stringEqualFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name = population", parser.getWhereClause());

        PropertyIsEqualTo intEqualFilter = ff.equals(ff.property("name"), ff.literal(7));

        q = new Query("counties", intEqualFilter);
        parser = new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name = 7", parser.getWhereClause());

        PropertyIsEqualTo floatEqualFilter = ff.equals(ff.property("name"), ff.literal(7.123));

        q = new Query("counties", floatEqualFilter);
        parser = new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name = 7.123", parser.getWhereClause());
    }

    @Test
    public void testPropertyValueEqualFilter() throws ParseException {
        PropertyIsEqualTo stringEqualFilter = ff.equals(ff.property("name"), ff.literal("abc"));
        Query q = new Query("counties", stringEqualFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name = 'abc'", parser.getWhereClause());
    }

    @Test
    public void testValueValueEqualFilter() throws ParseException {
        PropertyIsEqualTo stringEqualFilter = ff.equals(ff.literal("xyz"), ff.literal("abc"));
        Query q = new Query("counties", stringEqualFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("'xyz' = 'abc'", parser.getWhereClause());
    }

    @Test
    public void testPropertyNotEqualFilter() throws ParseException {
        PropertyIsNotEqualTo notEqualFilter =
                ff.notEqual(ff.property("name"), ff.property("population"));
        Query q = new Query("counties", notEqualFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name != population", parser.getWhereClause());
    }

    @Test
    public void testPropertyIsNullFilter() throws ParseException {
        PropertyIsNull nullFilter = ff.isNull(ff.property("name"));
        Query q = new Query("counties", nullFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name IS NULL", parser.getWhereClause());
    }

    @Test
    public void testNotFilter() throws ParseException {
        PropertyIsEqualTo stringEqualFilter = ff.equals(ff.property("name"), ff.literal("abc"));

        Not notFilter = ff.not(stringEqualFilter);
        Query q = new Query("counties", notFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("NOT ( name = 'abc' )", parser.getWhereClause());
    }

    @Test
    public void testPropertyGreaterThanFilter() throws ParseException {
        PropertyIsGreaterThan gtFilter = ff.greater(ff.property("population"), ff.literal(100));
        Query q = new Query("counties", gtFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("population > 100", parser.getWhereClause());
    }

    @Test
    public void testPropertyLessThanFilter() throws ParseException {
        PropertyIsLessThan ltFilter = ff.less(ff.property("population"), ff.literal(100.8));
        Query q = new Query("counties", ltFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("population < 100.8", parser.getWhereClause());
    }

    @Test
    public void testPropertyLessThanEqualFilter() throws ParseException {
        PropertyIsLessThanOrEqualTo ltFilter =
                ff.lessOrEqual(ff.property("population"), ff.literal(999));
        Query q = new Query("counties", ltFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("population <= 999", parser.getWhereClause());
    }

    @Test
    public void testPropertyGreaterThanEqualFilter() throws ParseException {
        PropertyIsGreaterThanOrEqualTo gtFilter =
                ff.greaterOrEqual(ff.property("population"), ff.literal(999));
        Query q = new Query("counties", gtFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("population >= 999", parser.getWhereClause());
    }

    @Test
    public void testPropertyLikeFilter() throws ParseException {
        PropertyIsLike likeFilter = ff.like(ff.property("name"), "nor%");
        Query q = new Query("counties", likeFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("name LIKE 'nor%'", parser.getWhereClause());
    }

    @Test
    public void testSpatialBBOXWithSimplify() {
        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

        BBOX bbox1 = ff.bbox("geom", -78.6785, 36.0049, -74.4158, 38.4493, "epsg:4326");

        Query q = new Query("counties", bbox1);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(
                        q, countiesFeatureType, CRS, BigqueryPregenerateOptions.MV_USE_EXISTING);

        assertEquals(
                "ST_INTERSECTSBOX(ST_SIMPLIFY(geom, 100), -78.678500, 36.004900, -74.415800, 38.449300)",
                parser.getWhereClause());
    }

    @Test
    public void testPropertyBetweenFilter() throws ParseException {
        FilterFactory ff = CommonFactoryFinder.getFilterFactory(null);

        PropertyIsBetween betweenFilter =
                ff.between(ff.property("population"), ff.literal(100), ff.literal(200));
        Query q = new Query("counties", betweenFilter);

        BigqueryFilterVisitor parser =
                new BigqueryFilterVisitor(q, countiesFeatureType, CRS, pregenNone);

        assertEquals("population BETWEEN 2022-01-01 AND 2022-12-31", parser.getWhereClause());
    }
}
