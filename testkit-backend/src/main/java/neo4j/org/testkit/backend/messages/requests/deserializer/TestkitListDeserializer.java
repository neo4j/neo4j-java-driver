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
import java.util.ArrayList;
import java.util.List;
import neo4j.org.testkit.backend.messages.requests.deserializer.types.CypherType;

public class TestkitListDeserializer extends StdDeserializer<List<?>> {
    @Serial
    private static final long serialVersionUID = -1878499456593526741L;

    private final TestkitCypherParamDeserializer mapDeserializer;

    public TestkitListDeserializer() {
        super(List.class);
        mapDeserializer = new TestkitCypherParamDeserializer();
    }

    @Override
    public List<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        List<Object> result = new ArrayList<>();

        var t = p.getCurrentToken();
        if (t == JsonToken.END_OBJECT) {
            return result;
        }
        if (t != JsonToken.START_ARRAY) {
            ctxt.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
        }

        var nextToken = p.nextToken();

        // standard list
        if (nextToken == JsonToken.VALUE_STRING) {
            while (nextToken != JsonToken.END_ARRAY && nextToken != null) {
                result.add(p.readValueAs(String.class));
                nextToken = p.nextToken();
            }
            return result;
        }

        // cypher parameter list
        while (nextToken != JsonToken.END_ARRAY) {
            String paramType;

            if (nextToken == JsonToken.START_OBJECT) {
                var fieldName = p.nextFieldName();
                if (fieldName.equals("name")) {
                    paramType = p.nextTextValue();
                    var mapValueType = cypherTypeToJavaType(paramType);
                    p.nextFieldName(); // next is data which we can drop
                    p.nextToken();
                    p.nextToken();
                    p.nextToken();
                    if (mapValueType == null) {
                        result.add(null);
                    } else {
                        if (paramType.equals("CypherMap")) // special recursive case for maps
                        {
                            result.add(mapDeserializer.deserialize(p, ctxt));
                        } else {
                            var obj = p.readValueAs(mapValueType);
                            if (obj instanceof CypherType) {
                                obj = ((CypherType) obj).asValue();
                            }
                            result.add(obj);
                        }
                    }
                }
            }
            nextToken = p.nextToken();
        }
        return result;
    }
}
