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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.Serial;
import org.neo4j.driver.internal.value.UnsupportedTypeValue;
import org.neo4j.driver.types.UnsupportedType;

public class TestkitUnsupportedTypeValueSerializer extends StdSerializer<UnsupportedTypeValue> {
    @Serial
    private static final long serialVersionUID = 4655180729235865377L;

    public TestkitUnsupportedTypeValueSerializer() {
        super(UnsupportedTypeValue.class);
    }

    @Override
    public void serialize(
            UnsupportedTypeValue unsupportedTypeValue, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {
        var unknown = unsupportedTypeValue.as(UnsupportedType.class);
        cypherObject(gen, "CypherUnsupportedType", () -> {
            gen.writeFieldName("name");
            gen.writeString(unknown.name());
            gen.writeFieldName("minimumProtocol");
            gen.writeString(unknown.minProtocolVersion());
            if (unknown.message().isPresent()) {
                var message = unknown.message().get();
                gen.writeFieldName("message");
                gen.writeObject(message);
            }
        });
    }
}
