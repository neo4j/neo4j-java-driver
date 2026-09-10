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
import java.util.HexFormat;
import java.util.Map;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherType;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;

public class TestkitCypherValueDeserializer extends StdDeserializer<Value> {
    @Serial
    private static final long serialVersionUID = -6981002212322046400L;

    public TestkitCypherValueDeserializer() {
        super(Value.class);
    }

    public TestkitCypherValueDeserializer(Class<Map> typeClass) {
        super(typeClass);
    }

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
        if ("CypherMap".equals(paramType)) { // Adapt this depending on the exact representation of CypherMap.
            return deserialize(data.traverse(p.getCodec()), ctxt);
        }

        var javaType = cypherTypeToJavaType(paramType);
        if (javaType == null) {
            return null;
        }

        var valueNode = data.get("value");
        if (valueNode == null) {
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
}
