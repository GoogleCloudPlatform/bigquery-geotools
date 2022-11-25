package org.geotools.data.bigquery;

import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.logging.Logger;
import org.geotools.data.Query;
import org.geotools.filter.FilterAttributeExtractor;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.And;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;

/**
 * Parse a geotools Query object and construct TableReadOptions that can be passed to
 * ReadSession.Builder.setReadOptions().
 *
 * @author traviswebb
 */
public class BigqueryQueryParser implements FilterVisitor {

    private static final Logger LOGGER = Logging.getLogger(BigqueryQueryParser.class);

    private final Query query;

    private Deque<String> clauseFragments;
    private TableReadOptions.Builder builder;
    private SimpleFeatureType schema;

    public BigqueryQueryParser(Query query, SimpleFeatureType schema) {
        this.query = query;
        this.schema = schema;
        this.builder = TableReadOptions.newBuilder();
        this.clauseFragments = new ArrayDeque<String>();
    }

    public BigqueryQueryParser parse() {
        query.getFilter().accept(this, null);

        builder.setRowRestriction(String.join(" ", clauseFragments));

        if (!query.retrieveAllProperties()) {
            builder.addAllSelectedFields(Arrays.asList(query.getPropertyNames()));
        }

        return this;
    }

    public TableReadOptions toReadOptions() throws IllegalStateException {
        return builder.build();
    }

    private String extractSingleAttribute(Filter filter) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor(schema);
        filter.accept(extractor, null);
        String[] attrs = extractor.getAttributeNames();

        if (attrs.length == 0) return null;
        else return attrs[0];
    }

    private String[] extractMultiAttribute(Filter filter) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor(schema);
        filter.accept(extractor, null);
        String[] attrs = extractor.getAttributeNames();

        return attrs;
    }

    private String getGeogFromGeojsonSQL(Geometry geom) {
        String geomJson = new GeometryJSON().toString(geom);
        return String.format("ST_GEOGFROMGEOJSON('%s', make_valid => true)", geomJson);
    }

    private String resolveValue(Object value) {
        if (value instanceof Geometry) {
            return getGeogFromGeojsonSQL((Geometry) value);
        } else if (value instanceof String) {
            return "'" + value + "'";
        } else if (value instanceof Integer) {
            return Integer.toString((Integer) value);
        } else if (value instanceof Float) {
            return Float.toString((Float) value);
        } else if (value instanceof Double) {
            return Double.toString((Double) value);
        } else {
            return (String) value;
        }
    }

    /**
     * Figures out filter arg types and positions, and return a PAIR of Strings that are formatted
     * and ready to be inserted into a clause template.
     *
     * @param filter
     * @param e1
     * @param e2
     * @return
     */
    private String[] getArgsFromBinaryFilter(Filter filter) {
        Object o1 = null;
        Object o2 = null;

        if (filter instanceof BinarySpatialOperator) {
            o1 = ((BinarySpatialOperator) filter).getExpression1().evaluate(null);
            o2 = ((BinarySpatialOperator) filter).getExpression2().evaluate(null);
        } else if (filter instanceof BinaryComparisonOperator) {
            o1 = ((BinaryComparisonOperator) filter).getExpression1().evaluate(null);
            o2 = ((BinaryComparisonOperator) filter).getExpression2().evaluate(null);
        }

        FilterAttributeExtractor extractor = new FilterAttributeExtractor(schema);
        filter.accept(extractor, null);
        String[] props = extractor.getAttributeNames();

        if (props.length == 2) {
            return props;
        }
        if (o1 != null && o2 != null) {
            return new String[] {resolveValue(o1), resolveValue(o2)};
        }

        if (o1 != null) {
            return new String[] {resolveValue(o1), props[0]};
        } else if (o2 != null) {
            return new String[] {props[0], resolveValue(o2)};
        } else {
            // should be impossible?
            return null;
        }
    }

    // SPATIAL FILTERS

    @Override
    public Object visit(BBOX filter, Object extraData) {
        Envelope envelope = filter.getExpression2().evaluate(null, Envelope.class);
        String geomAttr = extractSingleAttribute(filter);

        String clause =
                String.format(
                        "ST_INTERSECTSBOX(%s, %f, %f, %f, %f)",
                        geomAttr,
                        Math.max(envelope.getMinX(), -180),
                        Math.max(envelope.getMinY(), -90),
                        Math.min(envelope.getMaxX(), 180),
                        Math.min(envelope.getMaxY(), 90));

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Intersects filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_INTERSECTS(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(DWithin filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        double distance = filter.getDistance();
        // String distanceUnits = filter.getDistanceUnits();

        String clause = String.format("ST_DWITHIN(%s, %s, %f)", args[0], args[1], distance);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Contains filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_CONTAINS(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Disjoint filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_DISJOINT(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Touches filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_TOUCHES(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Equals filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_EQUALS(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(Within filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("ST_WITHIN(%s, %s)", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    // NON-SPATIAL COMPARISON FILTERS

    @Override
    public Object visit(PropertyIsEqualTo filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s = %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsNotEqualTo filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s != %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsNull filter, Object extraData) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor(schema);
        filter.accept(extractor, null);
        String[] props = extractor.getAttributeNames();

        String clause = String.format("%s IS NULL", props[0]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsGreaterThan filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s > %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsLessThan filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s < %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s >= %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsLessThanOrEqualTo filter, Object extraData) {
        String[] args = getArgsFromBinaryFilter(filter);
        String clause = String.format("%s <= %s", args[0], args[1]);

        clauseFragments.add(clause);

        return null;
    }

    @Override
    public Object visit(PropertyIsLike filter, Object extraData) {
        FilterAttributeExtractor extractor = new FilterAttributeExtractor(schema);
        filter.accept(extractor, null);
        String[] props = extractor.getAttributeNames();
        String clause = String.format("%s LIKE '%s'", props[0], filter.getLiteral());

        clauseFragments.add(clause);

        return null;
    }

    // BOOLEAN OPERATORS

    @Override
    public Object visit(Or filter, Object extraData) {
        List<Filter> children = filter.getChildren();
        if (children != null) {
            for (Filter child : children) {
                child.accept(this, null);
                clauseFragments.add("OR");
            }
            clauseFragments.removeLast();
        }
        return null;
    }

    @Override
    public Object visit(And filter, Object extraData) {
        List<Filter> children = filter.getChildren();
        if (children != null) {
            for (Filter child : children) {
                child.accept(this, null);
                clauseFragments.add("AND");
            }
            clauseFragments.removeLast();
        }
        return null;
    }

    @Override
    public Object visit(Not filter, Object extraData) {
        clauseFragments.add("NOT (");
        filter.getFilter().accept(this, extraData);
        clauseFragments.add(")");

        return null;
    }

    // UNSUPPORTED

    @Override
    public Object visitNullFilter(Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Beyond filter, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Crosses filter, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Overlaps filter, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(PropertyIsNil filter, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(After after, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(AnyInteracts anyInteracts, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Before before, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Begins begins, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(BegunBy begunBy, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(During during, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(EndedBy endedBy, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Ends ends, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(Meets meets, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(MetBy metBy, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(OverlappedBy overlappedBy, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(TContains contains, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(TEquals equals, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(TOverlaps contains, Object extraData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(PropertyIsBetween filter, Object extraData) {
        throw new UnsupportedOperationException();
    }

    // HANDLED ELSEWHERE

    @Override
    public Object visit(ExcludeFilter filter, Object extraData) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object visit(IncludeFilter filter, Object extraData) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object visit(Id filter, Object extraData) {
        // TODO Auto-generated method stub
        return null;
    }
}
