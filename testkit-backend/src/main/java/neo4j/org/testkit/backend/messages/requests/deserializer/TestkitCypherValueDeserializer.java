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

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherTypeToJavaType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.Serial;
import java.time.LocalDate;
import java.util.HexFormat;
import java.util.Map;
import lombok.Setter;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherDateTime;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherTime;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.InternalIsoDuration;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Vector;

public class TestkitCypherValueDeserializer extends StdDeserializer<Value> {
    @Serial
    private static final long serialVersionUID = -6981002212322046400L;

    public TestkitCypherValueDeserializer() {
        super(Value.class);
    }

    public TestkitCypherValueDeserializer(Class<Map> typeClass) {
        super(typeClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (!p.isExpectedStartObjectToken()) {
            ctxt.reportWrongTokenException(this, JsonToken.START_OBJECT, "Expected Cypher value object");
        }
        String paramType = null;
        JsonNode data = null;
        while (p.nextToken() != JsonToken.END_OBJECT) {
            var fieldName = p.currentName();
            p.nextToken();
            switch (fieldName) {
                case "name" -> paramType = p.getValueAsString();
                case "data" -> data = p.readValueAsTree();
                default -> p.skipChildren();
            }
        }
        if (paramType == null) {
            ctxt.reportInputMismatch(this, "Missing 'name' field");
        }
        if (data == null) {
            ctxt.reportInputMismatch(this, "Missing 'data' field");
        }
        if ("CypherMap".equals(paramType)) {
            var valueNode = data.get("value");
            if (valueNode == null) {
                ctxt.reportInputMismatch(this, "Missing 'value' inside 'data'");
            }

            var map = ctxt.readTreeAsValue(valueNode, Map.class);
            return Values.value(map);
        } else if ("CypherNull".equals(paramType)) {
            return Values.NULL;
        }

        var javaType = cypherTypeToJavaType(paramType);
        if (javaType == null) {
            return null;
        }

        var valueNode = data.get("value");
        if (valueNode == null) {
            if (javaType == IsoDuration.class) {
                var durationData = ctxt.readTreeAsValue(data, CypherDurationData.class);
                return Values.value(new InternalIsoDuration(
                        durationData.months, durationData.days, durationData.seconds, durationData.nanoseconds));
            } else if (javaType == CypherDateTime.class) {
                var dateTimeData = ctxt.readTreeAsValue(data, CypherDateTimeData.class);
                return new CypherDateTime(
                                dateTimeData.year,
                                dateTimeData.month,
                                dateTimeData.day,
                                dateTimeData.hour,
                                dateTimeData.minute,
                                dateTimeData.second,
                                dateTimeData.nanosecond,
                                dateTimeData.timezone_id,
                                dateTimeData.utc_offset_s)
                        .asValue();
            } else if (javaType == Vector.class) {
                var vectorData = ctxt.readTreeAsValue(data, TestkitCypherVectorDeserializer.CypherVectorData.class);
                return Values.value(TestkitCypherVectorDeserializer.deserialize(vectorData));
            } else if (javaType == CypherTime.class) {
                var timeData = ctxt.readTreeAsValue(data, TestkitCypherTimeDeserializer.CypherTimeData.class);
                return new CypherTime(
                                timeData.hour,
                                timeData.minute,
                                timeData.second,
                                timeData.nanosecond,
                                timeData.utc_offset_s)
                        .asValue();
            } else if (javaType == LocalDate.class) {
                var val = ctxt.readTreeAsValue(data, TestkitCypherDateDeserializer.CypherDateData.class);
                return Values.value(LocalDate.of(val.year, val.month, val.day));
            } else if (javaType == Point.class) {
                var val = ctxt.readTreeAsValue(data, TestkitCypherPointDeserializer.CypherPointData.class);
                var srid =
                        switch (val.system) {
                            case "cartesian" -> val.z == null ? 7203 : 9157;
                            case "wgs84" -> val.z == null ? 4326 : 4979;
                            default -> throw new IllegalArgumentException("Unknown coordinate system: " + val.system);
                        };

                return val.z == null ? Values.point(srid, val.x, val.y) : Values.point(srid, val.x, val.y, val.z);
            }

            ctxt.reportInputMismatch(this, "Missing 'value' inside 'data'");
        }

        Object obj;
        if (javaType == byte[].class) {
            var hex = valueNode.asText().replaceAll("\\s+", "");
            obj = HexFormat.of().parseHex(hex);
        } else {
            obj = ctxt.readTreeAsValue(valueNode, javaType);
        }

        if (obj instanceof CypherType cypherType) {
            return cypherType.asValue();
        }
        return Values.value(obj);
    }

    @Setter
    static final class CypherDurationData {
        Long months;
        Long days;
        Long seconds;
        Integer nanoseconds;
    }

    @Setter
    static final class CypherDateTimeData {
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
