package org.geotools.data.bigquery;

import static org.junit.Assert.assertArrayEquals;

import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Test;

public class BigqueryUtilTest {

    @Test
    public void testEnvelopeToExtent() {
        Double[] e1 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-180, -65, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e2 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(144, 180, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e3 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-65, 144, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e4 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-180, 199, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e5 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-5, 299, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e6 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-180, 180, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e7 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-65, -180, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e8 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(199, -144, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e9 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(5000, 365, -90, 90, DefaultGeographicCRS.WGS84));
        Double[] e10 =
                BigqueryUtil.gtEnvelopeToExtent(
                        new ReferencedEnvelope(-75, -65, -5, 5, DefaultGeographicCRS.WGS84));

        assertArrayEquals(new Double[] {-180.0, -90.0, -65.0, 90.0}, e1);
        assertArrayEquals(new Double[] {144.0, -90.0, 180.0, 90.0}, e2);
        assertArrayEquals(new Double[] {-65d, -90.0, 144d, 90.0}, e3);
        assertArrayEquals(new Double[] {-180.0, -90.0, 180.0, 90.0}, e4);
        assertArrayEquals(new Double[] {-5d, -90.0, 180d, 90.0}, e5);
        assertArrayEquals(new Double[] {-180d, -90.0, 180d, 90.0}, e6);
        assertArrayEquals(new Double[] {-180.0, -90.0, -65.0, 90.0}, e7);
        assertArrayEquals(new Double[] {-144.0, -90.0, 180.0, 90.0}, e8);
        assertArrayEquals(new Double[] {0d, 0d, 0d, 0d}, e9);
        assertArrayEquals(new Double[] {-75.0, -5.0, -65.0, 5.0}, e10);
    }
}
