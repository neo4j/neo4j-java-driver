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
package neo4j.org.testkit.backend.messages.requests.deserializer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class TestkitCypherDateTimeDeserializer extends StdDeserializer<ZonedDateTime> {
    public TestkitCypherDateTimeDeserializer() {
        super(ZonedDateTime.class);
    }

    @Override
    public ZonedDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {

        CypherDateTime dateTime = new CypherDateTime();

        JsonToken token = p.currentToken();
        while (token == JsonToken.FIELD_NAME
                || token == JsonToken.VALUE_NUMBER_INT
                || token == JsonToken.VALUE_STRING) {
            if (token == JsonToken.VALUE_NUMBER_INT) {
                String field = p.getCurrentName();
                int value = p.getValueAsInt();
                setField(dateTime, field, value);
            } else if (token == JsonToken.VALUE_STRING) {
                String field = p.getCurrentName();
                String value = p.getValueAsString();
                setField(dateTime, field, value);
            }
            token = p.nextToken();
        }

        ZoneId zoneId = dateTime.timezone_id != null
                ? ZoneId.of(dateTime.timezone_id)
                : ZoneOffset.ofTotalSeconds(dateTime.utc_offset_s);
        return ZonedDateTime.of(
                dateTime.year,
                dateTime.month,
                dateTime.day,
                dateTime.hour,
                dateTime.minute,
                dateTime.second,
                dateTime.nanosecond,
                zoneId);
    }

    private void setField(CypherDateTime dateTime, String fieldName, Object value) {
        try {
            Field field = CypherDateTime.class.getDeclaredField(fieldName);
            field.set(dateTime, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // ignored
        }
    }

    private static class CypherDateTime {
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
