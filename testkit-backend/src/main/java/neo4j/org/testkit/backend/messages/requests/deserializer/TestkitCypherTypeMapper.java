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
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import java.io.IOException;

public class TestkitCypherTypeMapper {
    public TestkitCypherTypeMapper() {}

    public <T> T mapData(JsonParser p, DeserializationContext ctxt, T data) throws IOException {
        var token = p.currentToken();
        while (token == JsonToken.FIELD_NAME
                || token == JsonToken.VALUE_NUMBER_INT
                || token == JsonToken.VALUE_STRING) {
            if (token == JsonToken.VALUE_NUMBER_INT) {
                var field = p.currentName();
                if (fieldIsType(data, field, Long.class)) {
                    setField(data, field, p.getLongValue());
                } else if (fieldIsType(data, field, Integer.class)) {
                    setField(data, field, p.getIntValue());
                } else {
                    throw new RuntimeException("Unhandled field type: " + field);
                }
            } else if (token == JsonToken.VALUE_STRING) {
                var field = p.currentName();
                var value = p.getValueAsString();
                setField(data, field, value);
            }
            token = p.nextToken();
        }
        return data;
    }

    private boolean fieldIsType(Object data, String field, Class<?> type) {
        try {
            var f = data.getClass().getDeclaredField(field);
            return f.getType().equals(type);
        } catch (NoSuchFieldException e) {
            return false;
        }
    }

    private void setField(Object data, String fieldName, Object value) {
        try {
            var field = data.getClass().getDeclaredField(fieldName);
            field.set(data, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(String.format(
                    "Received unexpected TestKit data field %s while parsing into %s",
                    fieldName, data.getClass().getName()));
        }
    }
}
