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
package neo4j.org.testkit.backend.messages.requests.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.Serial;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherDateTime;

public class TestkitCypherDateTimeDeserializer extends StdDeserializer<CypherDateTime> {
    @Serial
    private static final long serialVersionUID = 8980981682742682436L;

    @SuppressWarnings("serial")
    private final TestkitCypherTypeMapper mapper;

    public TestkitCypherDateTimeDeserializer() {
        super(CypherDateTime.class);
        mapper = new TestkitCypherTypeMapper();
    }

    @Override
    public CypherDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var data = mapper.mapData(p, ctxt, new CypherDateTimeData());
        return new CypherDateTime(
                data.year,
                data.month,
                data.day,
                data.hour,
                data.minute,
                data.second,
                data.nanosecond,
                data.timezone_id,
                data.utc_offset_s);
    }

    private static final class CypherDateTimeData {
        Integer year;
        Integer month;
        Integer day;
        Integer hour;
        Integer minute;
        Integer second;
        Integer nanosecond;
        Integer utc_offset_s;
        String timezone_id;
    }
}
