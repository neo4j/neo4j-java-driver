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

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherDateTime;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherTime;
import org.neo4j.driver.types.IsoDuration;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class GenUtils {
    static void object(JsonGenerator gen, RunnableWithIOException runnable) throws IOException {
        gen.writeStartObject();
        runnable.run();
        gen.writeEndObject();
    }

    public static <T> void list(JsonGenerator gen, List<T> list) throws IOException {
        gen.writeStartArray();
        for (var element : list) {
            gen.writeObject(element);
        }
        gen.writeEndArray();
    }

    public static <T> void cypherObject(JsonGenerator gen, String name, T value) throws IOException {
        cypherObject(gen, name, () -> gen.writeObjectField("value", value));
    }

    static <T> void cypherObject(JsonGenerator gen, String name, RunnableWithIOException runnable) throws IOException {
        object(gen, () -> {
            gen.writeStringField("name", name);
            gen.writeFieldName("data");
            object(gen, runnable);
        });
    }

    public static void writeDate(JsonGenerator gen, int year, int month, int day) throws IOException {
        gen.writeFieldName("year");
        gen.writeNumber(year);
        gen.writeFieldName("month");
        gen.writeNumber(month);
        gen.writeFieldName("day");
        gen.writeNumber(day);
    }

    public static void writeTime(JsonGenerator gen, int hour, int minute, int second, int nano) throws IOException {
        gen.writeFieldName("hour");
        gen.writeNumber(hour);
        gen.writeFieldName("minute");
        gen.writeNumber(minute);
        gen.writeFieldName("second");
        gen.writeNumber(second);
        gen.writeFieldName("nanosecond");
        gen.writeNumber(nano);
    }

    public static Class<?> cypherTypeToJavaType(String typeString) {
        return switch (typeString) {
            case "CypherBool" -> Boolean.class;
            case "CypherInt" -> Long.class;
            case "CypherFloat" -> Double.class;
            case "CypherString" -> String.class;
            case "CypherList" -> List.class;
            case "CypherMap" -> Map.class;
            case "CypherDateTime" -> CypherDateTime.class;
            case "CypherTime" -> CypherTime.class;
            case "CypherDate" -> LocalDate.class;
            case "CypherDuration" -> IsoDuration.class;
            default -> null;
        };
    }

    interface RunnableWithIOException {
        void run() throws IOException;
    }
}
