package org.geotools.data.bigquery;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class BigqueryDataStoreFactoryTest {
    @Test
    public void testCanProcess() {
        BigqueryDataStoreFactory factory = new BigqueryDataStoreFactory();

        Map<String, String> validMap = new HashMap<String, String>();
        validMap.put("projectId", "valid-project");
        validMap.put("datasetName", "valid_dataset");

        assertTrue(factory.canProcess(validMap));

        Map<String, String> invalidMap = new HashMap<String, String>();
        validMap.put("projectId", "12**&hello");
        validMap.put("datasetName", "garbage===");

        assertTrue(!factory.canProcess(invalidMap));
    }

    @Test
    public void testIsAvailable() {
        BigqueryDataStoreFactory factory = new BigqueryDataStoreFactory();

        assertTrue(factory.isAvailable());
    }
}
