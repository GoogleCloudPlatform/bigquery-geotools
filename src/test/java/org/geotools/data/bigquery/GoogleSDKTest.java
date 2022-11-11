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
