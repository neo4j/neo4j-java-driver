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
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.io.Serial;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherType;

public class TestkitCypherParamDeserializer extends StdDeserializer<Map<String, Object>> {
    @Serial
    private static final long serialVersionUID = -3239342714470961079L;

    public TestkitCypherParamDeserializer() {
        super(Map.class);
    }

    public TestkitCypherParamDeserializer(Class<Map> typeClass) {
        super(typeClass);
    }

    @Override
    public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Map<String, Object> result = new HashMap<>();

        String key;
        if (p.isExpectedStartObjectToken()) {
            key = p.nextFieldName();
        } else {
            var t = p.getCurrentToken();
            if (t == JsonToken.END_OBJECT) {
                return Collections.emptyMap();
            }
            if (t != JsonToken.FIELD_NAME) {
                ctxt.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
            }
            key = p.currentName();
        }

        for (; key != null; key = p.nextFieldName()) {
            String paramType;

            if (p.nextToken() == JsonToken.START_OBJECT) {
                var fieldName = p.nextFieldName();
                if (fieldName.equals("name")) {
                    paramType = p.nextTextValue();
                    var mapValueType = cypherTypeToJavaType(paramType);
                    p.nextFieldName(); // next is data which we can drop
                    p.nextToken();
                    p.nextToken();
                    p.nextToken();
                    if (mapValueType == null) {
                        result.put(key, null);
                    } else {
                        if (paramType.equals("CypherMap")) // special recursive case for maps
                        {
                            result.put(key, deserialize(p, ctxt));
                        } else {
                            var obj = p.readValueAs(mapValueType);
                            if (obj instanceof CypherType) {
                                obj = ((CypherType) obj).asValue();
                            }
                            result.put(key, obj);
                        }
                    }
                }
            }
            p.nextToken(); // close value
            p.nextToken(); // close map
        }

        return result;
    }
}
