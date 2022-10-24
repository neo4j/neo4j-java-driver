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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.summary.ResultSummary;

class EagerResultTransformerTest {
    @Test
    void shouldReturnEagerResult() {
        var transformer = new EagerResultTransformer();
        var result = mock(Result.class);
        var keys = List.of("key");
        var records = List.of(mock(Record.class));
        var summary = mock(ResultSummary.class);
        given(result.keys()).willReturn(keys);
        given(result.list()).willReturn(records);
        given(result.consume()).willReturn(summary);

        var eagerResult = transformer.transform(result);

        then(result).should().keys();
        then(result).should().list();
        then(result).should().consume();
        assertEquals(keys, eagerResult.keys());
        assertEquals(records, eagerResult.records());
        assertEquals(summary, eagerResult.summary());
    }
}
