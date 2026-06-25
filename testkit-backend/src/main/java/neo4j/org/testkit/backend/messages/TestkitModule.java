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
package neo4j.org.testkit.backend.messages;

import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.Serial;
import java.time.LocalDate;
import java.util.List;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherDateDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherDateTimeDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherDurationDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitCypherTimeDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitListDeserializer;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherDateTime;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherTime;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitDateTimeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitDateValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitDurationValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitListValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitLocalDateTimeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitLocalTimeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitMapValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitNodeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitPathValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitRecordSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitRelationshipValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitTimeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitValueSerializer;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.DateTimeValue;
import org.neo4j.driver.internal.value.DateValue;
import org.neo4j.driver.internal.value.DurationValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.LocalDateTimeValue;
import org.neo4j.driver.internal.value.LocalTimeValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.TimeValue;
import org.neo4j.driver.types.IsoDuration;

public class TestkitModule extends SimpleModule {
    @Serial
    private static final long serialVersionUID = 7051732358423895087L;

    @SuppressWarnings("this-escape")
    public TestkitModule() {
        this.addDeserializer(List.class, new TestkitListDeserializer());
        this.addDeserializer(CypherDateTime.class, new TestkitCypherDateTimeDeserializer());
        this.addDeserializer(CypherTime.class, new TestkitCypherTimeDeserializer());
        this.addDeserializer(IsoDuration.class, new TestkitCypherDurationDeserializer());
        this.addDeserializer(LocalDate.class, new TestkitCypherDateDeserializer());

        this.addSerializer(Value.class, new TestkitValueSerializer());
        this.addSerializer(NodeValue.class, new TestkitNodeValueSerializer());
        this.addSerializer(ListValue.class, new TestkitListValueSerializer());
        this.addSerializer(DateTimeValue.class, new TestkitDateTimeValueSerializer());
        this.addSerializer(DateValue.class, new TestkitDateValueSerializer());
        this.addSerializer(DurationValue.class, new TestkitDurationValueSerializer());
        this.addSerializer(LocalDateTimeValue.class, new TestkitLocalDateTimeValueSerializer());
        this.addSerializer(LocalTimeValue.class, new TestkitLocalTimeValueSerializer());
        this.addSerializer(TimeValue.class, new TestkitTimeValueSerializer());
        this.addSerializer(Record.class, new TestkitRecordSerializer());
        this.addSerializer(MapValue.class, new TestkitMapValueSerializer());
        this.addSerializer(PathValue.class, new TestkitPathValueSerializer());
        this.addSerializer(RelationshipValue.class, new TestkitRelationshipValueSerializer());
    }
}
