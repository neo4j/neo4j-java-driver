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
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.types.IsoDuration;

public class TestkitCypherDurationDeserializer extends StdDeserializer<IsoDuration> {
    @Serial
    private static final long serialVersionUID = 3128342779161014747L;

    @SuppressWarnings("serial")
    private final TestkitCypherTypeMapper mapper;

    public TestkitCypherDurationDeserializer() {
        super(IsoDuration.class);
        mapper = new TestkitCypherTypeMapper();
    }

    public IsoDuration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        var data = mapper.mapData(p, ctxt, new CypherDurationData());
        return new InternalIsoDuration(data.months, data.days, data.seconds, data.nanoseconds);
    }

    private static final class CypherDurationData {
        Long months;
        Long days;
        Long seconds;
        Integer nanoseconds;
    }
}
