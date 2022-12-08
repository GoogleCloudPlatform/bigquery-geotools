package org.geotools.data.bigquery;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class BigqueryDataStoreFactoryTest {
    @Test
    public void testCanProcess() {
        BigqueryDataStoreFactory factory = new BigqueryDataStoreFactory();

        Map<String, Object> validMap = new HashMap<String, Object>();
        validMap.put("Project Id", "valid-project");
        validMap.put("Dataset Name", "valid_dataset");
        validMap.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        assertTrue(factory.canProcess(validMap));

        Map<String, String> invalidMap = new HashMap<String, String>();
        validMap.put("Project Id", "12**&hello");
        validMap.put("Dataset Name", "garbage===");
        validMap.put("Access Method", BigqueryAccessMethod.STANDARD_QUERY_API);

        assertTrue(!factory.canProcess(invalidMap));
    }

    @Test
    public void testIsAvailable() {
        BigqueryDataStoreFactory factory = new BigqueryDataStoreFactory();

        assertTrue(factory.isAvailable());
    }
}
