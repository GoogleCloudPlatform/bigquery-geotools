package org.geotools.data.bigquery;

import org.locationtech.jts.geom.Envelope;

public class BigqueryUtil {

    /**
     * Calculate safe BOX bounds for ST_INTERSECTSBOX and other BQ BOX queries, per:
     *
     * <p>https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_intersectsbox
     * https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_boundingbox
     *
     * @param env
     * @return
     */
    protected static Double[] gtEnvelopeToExtent(Envelope env) {
        // bound latitudes by [-90,90]
        double minY = Math.max(-90, env.getMinY());
        double maxY = Math.min(90, env.getMaxY());

        // if given a value outside [-180,360], there is no way to make any
        // sense of what it should mean, so clip everything outside
        double minX = Math.min(360, Math.max(-180, env.getMinX()));
        double maxX = Math.min(360, env.getMaxX());

        if (minX == maxX) {
            return new Double[] {0d, 0d, 0d, 0d};
        } else if ((maxX - minX) >= 360) {
            // if x range is larger than the globe, return the whole globe
            minX = -180;
            maxX = 180;
        } else if (minX < 0 && maxX > 180) {
            // if one value is negative but the other is >180, then clip at 180deg.
            maxX = 180;
        } else if (minX > 180 && maxX > 180) {
            // if both values are >180, reduce to the (-180,180) range
            minX = minX - 360;
            maxX = maxX - 360;
        } else if (minX > 0 && maxX > 180) {
            // if longitudes in the (0,360) range, swap and shift by half globe to
            // represent the same bbox in the (-180,180) range.
            minX = maxX - 180;
            maxX = minX - 180;
        }

        return new Double[] {minX, minY, maxX, maxY};
    }
}
