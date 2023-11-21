/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver;

import java.util.List;
import org.neo4j.driver.summary.ResultSummary;

/**
 * An in-memory result of executing a Cypher query that has been consumed in full.
 * @since 5.5
 */
public interface EagerResult {
    /**
     * Returns the keys of the records this result contains.
     *
     * @return list of keys
     */
    List<String> keys();

    /**
     * Returns the list of records this result contains.
     *
     * @return list of records
     */
    List<Record> records();

    /**
     * Returns the result summary.
     *
     * @return result summary
     */
    ResultSummary summary();
}
