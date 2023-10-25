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
package org.neo4j.driver.internal.summary;

import java.util.stream.IntStream;
import org.neo4j.driver.summary.SummaryCounters;

public record InternalSummaryCounters(
        int nodesCreated,
        int nodesDeleted,
        int relationshipsCreated,
        int relationshipsDeleted,
        int propertiesSet,
        int labelsAdded,
        int labelsRemoved,
        int indexesAdded,
        int indexesRemoved,
        int constraintsAdded,
        int constraintsRemoved,
        int systemUpdates)
        implements SummaryCounters {
    public static final InternalSummaryCounters EMPTY_STATS =
            new InternalSummaryCounters(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    @Override
    public boolean containsUpdates() {
        return IntStream.of(
                        nodesCreated,
                        nodesDeleted,
                        relationshipsCreated,
                        relationshipsDeleted,
                        propertiesSet,
                        labelsAdded,
                        labelsRemoved,
                        indexesAdded,
                        indexesRemoved,
                        constraintsAdded,
                        constraintsRemoved)
                .anyMatch(this::isPositive);
    }

    @Override
    public boolean containsSystemUpdates() {
        return isPositive(systemUpdates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        var that = (InternalSummaryCounters) o;

        return nodesCreated == that.nodesCreated
                && nodesDeleted == that.nodesDeleted
                && relationshipsCreated == that.relationshipsCreated
                && relationshipsDeleted == that.relationshipsDeleted
                && propertiesSet == that.propertiesSet
                && labelsAdded == that.labelsAdded
                && labelsRemoved == that.labelsRemoved
                && indexesAdded == that.indexesAdded
                && indexesRemoved == that.indexesRemoved
                && constraintsAdded == that.constraintsAdded
                && constraintsRemoved == that.constraintsRemoved
                && systemUpdates == that.systemUpdates;
    }

    private boolean isPositive(int value) {
        return value > 0;
    }

    @Override
    public String toString() {
        return "InternalSummaryCounters{" + "nodesCreated="
                + nodesCreated + ", nodesDeleted="
                + nodesDeleted + ", relationshipsCreated="
                + relationshipsCreated + ", relationshipsDeleted="
                + relationshipsDeleted + ", propertiesSet="
                + propertiesSet + ", labelsAdded="
                + labelsAdded + ", labelsRemoved="
                + labelsRemoved + ", indexesAdded="
                + indexesAdded + ", indexesRemoved="
                + indexesRemoved + ", constraintsAdded="
                + constraintsAdded + ", constraintsRemoved="
                + constraintsRemoved + ", systemUpdates="
                + systemUpdates + '}';
    }
}
