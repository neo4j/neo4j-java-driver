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
package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.Setter;
import neo4j.org.testkit.backend.messages.AbstractResultNext;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

@Setter
@Getter
public class ResultNext extends AbstractResultNext {
    private static final String DATE_TIME = "DATE_TIME";
    private ResultNextBody data;

    @Override
    protected neo4j.org.testkit.backend.messages.responses.TestkitResponse createResponse(Record record) {
        record.values().stream().filter(v -> DATE_TIME.equals(v.type().name())).forEach(Value::asObject);
        return neo4j.org.testkit.backend.messages.responses.Record.builder()
                .data(neo4j.org.testkit.backend.messages.responses.Record.RecordBody.builder()
                        .values(record)
                        .build())
                .build();
    }

    @Override
    protected String getResultId() {
        return data.getResultId();
    }

    @Setter
    @Getter
    public static class ResultNextBody {
        private String resultId;
    }
}
