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

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.factory.CommonFactoryFinder;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;

public abstract class BigqueryFeatureReader implements SimpleFeatureReader {

    protected BigqueryDataStore store;

    protected SimpleFeatureType featureType;
    protected ContentState state;
    protected int rowIndex;
    protected int rowLimit;

    protected final int srid;

    protected final String geomColumn;
    protected final Query query;

    /**
     * Set up BigQuery Storage API read session
     *
     * @param state
     * @param tableId
     * @param query
     * @throws IOException
     */
    public BigqueryFeatureReader(ContentState state, Query query) throws IOException {
        this.store = (BigqueryDataStore) state.getEntry().getDataStore();
        this.state = state;
        this.featureType = state.getFeatureType();
        this.srid = store.SRID;
        this.geomColumn = featureType.getGeometryDescriptor().getLocalName();
        this.rowIndex = -1;
        this.rowLimit = query.getMaxFeatures();
        this.query = decorateQuery(featureType, query);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public void close() throws IOException {}

    /**
     * Determine any requirements for querying against the given BigQuery table, and decorate the
     * Query object where necessary.
     *
     * @param type
     * @param query
     * @return
     */
    private Query decorateQuery(SimpleFeatureType type, Query query) {
        for (AttributeDescriptor attr : type.getAttributeDescriptors()) {
            Map<Object, Object> userData = attr.getUserData();

            if ((Boolean) userData.get("partitioningRequired")
                    && store.autoAddRequiredPartitionFilter) {
                decorateQueryWithPartitionFilter(attr, query);
            }
        }

        return query;
    }

    /**
     * Decorate the query object with the required partition filter.
     *
     * @param type
     * @param query
     * @return
     */
    private void decorateQueryWithPartitionFilter(AttributeDescriptor attr, Query query) {
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2(null);

        String column = attr.getLocalName();
        Map<Object, Object> userData = attr.getUserData();
        String partitionType = (String) userData.get("partitioningType");

        Instant now = Instant.now();
        Instant partitionOperand;
        if ("HOUR".equals(partitionType)) {
            partitionOperand = now.minus(1, ChronoUnit.HOURS);
        } else if ("DAY".equals(partitionType)) {
            partitionOperand = now.minus(1, ChronoUnit.DAYS);
        } else if ("MONTH".equals(partitionType)) {
            partitionOperand = now.minus(1, ChronoUnit.MONTHS);
        } else {
            partitionOperand = now.minus(1, ChronoUnit.YEARS);
        }
        Date partitionDate = Date.from(partitionOperand);

        Filter partitionFilter = ff.greaterOrEqual(ff.property(column), ff.literal(partitionDate));
        if (query.getFilter() == Filter.INCLUDE) {
            query.setFilter(partitionFilter);
        } else {
            query.setFilter(ff.and(query.getFilter(), partitionFilter));
        }
    }
}
