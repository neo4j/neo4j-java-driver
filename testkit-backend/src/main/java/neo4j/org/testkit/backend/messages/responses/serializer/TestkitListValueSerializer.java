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
package neo4j.org.testkit.backend.messages.responses.serializer;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherObject;
import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.list;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.Serial;
import java.util.function.Function;
import org.neo4j.driver.internal.value.ListValue;

public class TestkitListValueSerializer extends StdSerializer<ListValue> {
    @Serial
    private static final long serialVersionUID = -5564826952797323279L;

    public TestkitListValueSerializer() {
        super(ListValue.class);
    }

    @Override
    public void serialize(ListValue listValue, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        cypherObject(gen, "CypherList", () -> {
            gen.writeFieldName("value");
            list(gen, listValue.asList(Function.identity()));
        });
    }
}
