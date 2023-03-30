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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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

    @Test
    // @SetEnvironmentVariable(key = "GEOSERVER_DATA_DIR", value = "/opt/geoserver/data_dir")
    public void testGetKeyFileFromPath() throws IOException {
        String linuxPath1 = "file:///Users/test/workspace/key.json";
        String linuxPath2 = "file://src/test/resources/testkey.json";
        String windowsPath1 = "file://D:\\Workspace\\key.json";
        String dataDirPath = "file:key.json";

        BigqueryDataStoreFactory factory = new BigqueryDataStoreFactory();

        assertEquals("/Users/test/workspace/key.json", factory.getCompatibleKeyPath(linuxPath1));
        assertEquals("src/test/resources/testkey.json", factory.getCompatibleKeyPath(linuxPath2));
        assertEquals("D:\\Workspace\\key.json", factory.getCompatibleKeyPath(windowsPath1));
        assertEquals("/opt/geoserver/data_dir/key.json", factory.getCompatibleKeyPath(dataDirPath));
    }
}
