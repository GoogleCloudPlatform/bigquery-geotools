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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.common.collect.ImmutableMap;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.logging.Logging;
import org.opengis.feature.type.Name;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/** Geotools datastore for BigQuery */
public class BigqueryDataStore extends ContentDataStore {

    private static final Logger LOGGER = Logging.getLogger(BigqueryDataStore.class);

    /** Parameterize this if/when BQ supports non-WGS84 SRIDs. Constant for now. */
    protected final int SRID = 4326;

    protected final CoordinateReferenceSystem CRS = DefaultGeographicCRS.WGS84;

    protected BigQuery queryClient;
    protected BigQueryReadClient storageClient;

    protected final String projectId;
    protected final String datasetName;
    protected final File serviceAccountKeyFile;
    protected final BigqueryAccessMethod accessMethod;
    protected final Boolean simplify;
    protected final Boolean useQueryCache;
    protected final Boolean autoAddRequiredPartitionFilter;
    protected final Integer jobTimeoutSeconds;
    protected final BigqueryPregenerateOptions pregen;

    protected GoogleCredentials credentials;

    /** Table "types" to support in geoserver. */
    protected static final Map<TableDefinition.Type, String> TABLE_TYPE_MAP =
            new ImmutableMap.Builder<TableDefinition.Type, String>()
                    .put(TableDefinition.Type.TABLE, "Table")
                    .put(TableDefinition.Type.VIEW, "View")
                    .put(TableDefinition.Type.MATERIALIZED_VIEW, "Materialized View")
                    // .put(TableDefinition.Type.EXTERNAL, "External")
                    // .put(TableDefinition.Type.MODEL, "ML Model")
                    .build();

    /**
     * Construct a BigqueryDatastore for a given project and dataset.
     *
     * @param projectId
     * @param datasetName
     */
    public BigqueryDataStore(
            String projectId,
            String datasetName,
            BigqueryAccessMethod accessMethod,
            Boolean simplify,
            Boolean useQueryCache,
            Boolean autoAddRequiredPartitionFilter,
            Integer jobTimeoutSeconds,
            BigqueryPregenerateOptions pregen,
            File serviceAccountKeyFile)
            throws IOException {

        this.setNamespaceURI(null);

        this.projectId = projectId;
        this.datasetName = datasetName;
        this.accessMethod = accessMethod;
        this.simplify = simplify == null ? false : simplify;
        this.useQueryCache = useQueryCache == null ? false : useQueryCache;
        this.jobTimeoutSeconds = jobTimeoutSeconds;
        this.pregen = pregen;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.autoAddRequiredPartitionFilter =
                autoAddRequiredPartitionFilter == null ? false : autoAddRequiredPartitionFilter;

        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
        BigQueryReadSettings.Builder settingsBuilder = BigQueryReadSettings.newBuilder();

        if (serviceAccountKeyFile != null) {
            try (FileInputStream serviceAccountStream =
                    new FileInputStream(serviceAccountKeyFile)) {
                credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
            }
            builder.setCredentials(credentials);
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }

        this.queryClient = builder.setProjectId(projectId).build().getService();

        if (accessMethod == BigqueryAccessMethod.STORAGE_API) {
            this.storageClient = BigQueryReadClient.create(settingsBuilder.build());
            LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());
        }
    }

    @Override
    /** Return list of BQ tables in the given dataset that contain a GEOGRAPHY column. */
    protected List<Name> createTypeNames() throws IOException {
        List<Name> typeNames = new ArrayList<>();
        try {
            Page<Table> tables = queryClient.listTables(datasetName, TableListOption.pageSize(100));
            for (Table table : tables.iterateAll()) {
        	TableId tableId = table.getTableId();
                Table hydratedTable = queryClient.getTable(tableId);
                TableDefinition tableDef = hydratedTable.getDefinition();
                Schema schema = tableDef.getSchema();
                TableDefinition.Type type = tableDef.getType();
                String geomColumn = getTableGeometryColumn(schema);

                if (TABLE_TYPE_MAP.containsKey(type) && null != geomColumn
                	&& tableId.toString().indexOf("_pregen_") == -1) {
                    typeNames.add(new NameImpl(getTypeLabel(table.getGeneratedId())));
                }
            }
        } catch (BigQueryException e) {
            throw new IOException(e);
        }

        return typeNames;
    }

    public static String getTypeLabel(String fullTableName) {
        return fullTableName.replace(":", ".");
    }

    protected String getTableGeometryColumn(Schema schema) {
        FieldList fields = schema.getFields();
        for (Field field : fields) {
            StandardSQLTypeName fieldType = field.getType().getStandardType();
            if (fieldType == StandardSQLTypeName.GEOGRAPHY) {
                return field.getName();
            }
        }

        return null;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new BigqueryFeatureSource(entry);
    }

    BigQuery read() throws IOException {
        return queryClient;
    }
}
