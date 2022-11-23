package org.geotools.data.bigquery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ServerStream;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.PickFirstLoadBalancerProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.logging.Logging;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

public class BigqueryFeatureReader implements SimpleFeatureReader {

    private static final Logger LOGGER = Logging.getLogger(BigqueryFeatureReader.class);

    private SimpleFeatureType featureType;
    protected ContentState state;
    private int rowIndex;

    private ServerStream<ReadRowsResponse> stream;
    private Iterator<ReadRowsResponse> streamIterator;
    private final int srid;
    private final String geomColumn;
    private final Query query;

    private final BigQueryReadClient client;
    private final BigqueryAvroReader reader;

    /**
     * Set up BigQuery Storage API read session
     *
     * @param state
     * @param tableId
     * @param query
     * @throws IOException
     */
    public BigqueryFeatureReader(
            ContentState state, String projectUri, String tableUri, Query query)
            throws IOException {

        /*
        System.out.println(projectUri);
        System.out.println(tableUri);
        System.out.println(query);
        System.out.println(query.getProperties());
        System.out.println(query.getPropertyNames());
        */

        this.query = query;
        this.state = state;
        this.featureType = state.getFeatureType();
        this.srid = ((BigqueryDataStore) state.getEntry().getDataStore()).SRID;
        this.geomColumn = ((BigqueryDataStore) state.getEntry().getDataStore()).GEOM_COLUMN;
        this.rowIndex = -1;

        LoadBalancerRegistry.getDefaultRegistry().register(new PickFirstLoadBalancerProvider());

        Credentials credentials = ((BigqueryDataStore) state.getEntry().getDataStore()).credentials;
        BigQueryReadSettings.Builder settingsBuilder = BigQueryReadSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }

        client = BigQueryReadClient.create(settingsBuilder.build());
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(tableUri)
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(parseQuery());

        // TODO configure snapshot time
        CreateReadSessionRequest.Builder builder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(projectUri)
                        .setReadSession(sessionBuilder)
                        .setMaxStreamCount(1);

        ReadSession session = client.createReadSession(builder.build());

        this.reader =
                new BigqueryAvroReader(
                        new Schema.Parser().parse(session.getAvroSchema().getSchema()));

        Preconditions.checkState(session.getStreamsCount() > 0);
        String streamName = session.getStreams(0).getName();

        ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName).build();

        this.stream = client.readRowsCallable().call(readRowsRequest);
        this.streamIterator = stream.iterator();
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public SimpleFeature next()
            throws IOException, IllegalArgumentException, NoSuchElementException {

        if (!reader.hasNext()) {
            reader.decodeRows(streamIterator.next().getAvroRows());
        }

        return parseFeature(
                reader.next(), ++rowIndex, featureType, reader.getSchemaKeys(), srid, geomColumn);
    }

    @Override
    public boolean hasNext() throws IOException {
        return reader.hasNext() || streamIterator.hasNext();
    }

    @Override
    public void close() throws IOException {
        client.shutdownNow();
    }

    /**
     * Return BQ TableReadOptions from the given Query.
     *
     * @return
     */
    private TableReadOptions parseQuery() {
        BigqueryQueryParser parser = new BigqueryQueryParser(query, getFeatureType());
        return parser.parse().toReadOptions();
    }

    private static SimpleFeature parseFeature(
            GenericRecord row,
            int rowIndex,
            SimpleFeatureType featureType,
            List<String> keys,
            int srid,
            String geomColumn)
            throws IOException {

        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);

        for (String column : keys) {
            if (column == geomColumn) continue;

            builder.set(column, row.get(column));
        }

        String geomWkt = row.get(geomColumn).toString();

        try {
            Geometry geom = new WKTReader().read(geomWkt);
            geom.setSRID(srid);
            builder.set(geomColumn, geom);
        } catch (ParseException e) {
            throw new IOException(e);
        }
        return builder.buildFeature(Integer.toString(rowIndex));
    }

    private class BigqueryAvroReader {

        private final DatumReader<GenericRecord> datumReader;
        private BinaryDecoder decoder = null;
        private GenericRecord row = null;
        private Schema avroSchema;
        private List<String> schemaKeys;

        public BigqueryAvroReader(Schema avroSchema) {
            Preconditions.checkNotNull(avroSchema);
            this.avroSchema = avroSchema;
            this.datumReader = new GenericDatumReader<>(avroSchema);
        }

        public List<String> getSchemaKeys() {
            if (schemaKeys != null) return schemaKeys;

            schemaKeys = new ArrayList<String>();
            for (Schema.Field field : avroSchema.getFields()) {
                schemaKeys.add(field.name());
            }
            return schemaKeys;
        }

        public void decodeRows(AvroRows avroRows) {
            decoder =
                    DecoderFactory.get()
                            .binaryDecoder(
                                    avroRows.getSerializedBinaryRows().toByteArray(), decoder);
        }

        public GenericRecord next() throws IOException {
            return datumReader.read(row, decoder);
        }

        public boolean hasNext() throws IOException {
            return decoder != null && !decoder.isEnd();
        }
    }
}
