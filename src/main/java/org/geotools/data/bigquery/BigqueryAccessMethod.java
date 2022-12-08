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

public enum BigqueryAccessMethod {
    /**
     * https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1
     *
     * <p>Faster access to storage, less flexibility.
     */
    STORAGE_API,

    /**
     * https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/overview
     *
     * <p>Slower, more flexible in ability to simplify and perform other in-database calculations.
     */
    STANDARD_QUERY_API
}
