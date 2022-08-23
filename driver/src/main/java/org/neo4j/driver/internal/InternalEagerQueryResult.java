/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.internal;

import java.util.List;
import org.neo4j.driver.EagerQueryResult;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.summary.ResultSummary;

public record InternalEagerQueryResult(List<String> keys, List<Record> records, ResultSummary summary)
        implements EagerQueryResult {
    @Override
    public Record single() {
        var size = records.size();
        if (size != 1) {
            throw new NoSuchRecordException(String.format(
                    """
                    Expected a result with a single record, but this result \
                    contains %d. Ensure your query returns only \
                    one record.""",
                    records.size()));
        } else {
            return records.get(0);
        }
    }

    @Override
    public Value scalar() {
        var record = single();
        if (record.size() != 1) {
            // todo determine right exception
            throw new NoSuchRecordException("Result's records contained more than a single column.");
        }
        return record.get(0);
    }
}
