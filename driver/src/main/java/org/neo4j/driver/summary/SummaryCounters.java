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
package org.neo4j.driver.summary;

import org.neo4j.driver.util.Immutable;

/**
 * Contains counters for various operations that a query triggered.
 * @since 1.0
 */
@Immutable
public interface SummaryCounters {
    /**
     * Whether there were any updates at all, e.g. any of the counters are greater than 0.
     *
     * @return true if the query made any updates
     */
    boolean containsUpdates();

    /**
     * Returns the number of nodes created.
     *
     * @return the number of nodes created
     */
    int nodesCreated();

    /**
     * Returns the number of nodes deleted.
     *
     * @return the number of nodes deleted
     */
    int nodesDeleted();

    /**
     * Returns the number of relationships created.
     *
     * @return the number of relationships created
     */
    int relationshipsCreated();

    /**
     * Returns the number of relationships deleted.
     *
     * @return the number of relationships deleted
     */
    int relationshipsDeleted();

    /**
     * Returns the number of properties (on both nodes and relationships) set.
     *
     * @return the number of properties (on both nodes and relationships) set
     */
    int propertiesSet();

    /**
     * Returns the number of labels added to nodes.
     *
     * @return the number of labels added to nodes
     */
    int labelsAdded();

    /**
     * Returns the number of labels removed from nodes.
     *
     * @return the number of labels removed from nodes
     */
    int labelsRemoved();

    /**
     * Returns the number of indexes added to the schema.
     *
     * @return the number of indexes added to the schema
     */
    int indexesAdded();

    /**
     * Returns the number of indexes removed from the schema.
     *
     * @return the number of indexes removed from the schema
     */
    int indexesRemoved();

    /**
     * Returns the number of constraints added to the schema.
     *
     * @return the number of constraints added to the schema
     */
    int constraintsAdded();

    /**
     * Returns the number of constraints removed from the schema.
     *
     * @return the number of constraints removed from the schema
     */
    int constraintsRemoved();

    /**
     * If the query updated the system graph in any way, this method will return true,
     *
     * @return true if the system graph has been updated.
     */
    boolean containsSystemUpdates();

    /**
     * Returns the number of system updates performed by this query.
     *
     * @return the number of system updates performed by this query
     */
    int systemUpdates();
}
