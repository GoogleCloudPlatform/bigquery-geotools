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

import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.io.IOException;
import java.util.UUID;
import org.junit.Test;

/** Unit test for simple App. */
public class GoogleSDKTest {
    /**
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testBQConnection() throws InterruptedException, IOException {
        String projectId = "bigquery-geotools";
        BigQuery bigquery =
                BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(
                                "SELECT commit, author, repo_name "
                                        + "FROM `bigquery-public-data.github_repos.commits` "
                                        + "WHERE subject like '%bigquery%' "
                                        + "ORDER BY subject DESC LIMIT 10")
                        .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        TableResult result = queryJob.getQueryResults();
        assertTrue(result.getTotalRows() == 10);
    }

    public void testGeotools() throws InterruptedException, IOException {}
}
