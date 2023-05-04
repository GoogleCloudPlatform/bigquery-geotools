/*
 * Copyright 2023 Google LLC
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

public enum BigqueryPregenerateOptions {
    /** Do not create or use any pregenerated materialized views */
    MV_NONE,
    
    /** utilize any existing materialized views, but do not generate any. */
    MV_USE_EXISTING,

    /** pregenerate 4 tables of simplified geometries at tolerances 1m, 10m, 100m, and 1000m. */
    MV_PREGEN_ALL
}