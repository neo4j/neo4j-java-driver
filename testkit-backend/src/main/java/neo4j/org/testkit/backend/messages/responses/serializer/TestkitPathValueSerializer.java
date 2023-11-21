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
import java.util.stream.StreamSupport;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.internal.value.RelationshipValue;

public class TestkitPathValueSerializer extends StdSerializer<PathValue> {
    @Serial
    private static final long serialVersionUID = -6937949051813143300L;

    public TestkitPathValueSerializer() {
        super(PathValue.class);
    }

    @Override
    public void serialize(PathValue pathValue, JsonGenerator gen, SerializerProvider provider) throws IOException {
        cypherObject(gen, "Path", () -> {
            var path = pathValue.asPath();
            var nodes = StreamSupport.stream(path.nodes().spliterator(), false)
                    .map(NodeValue::new)
                    .toArray(NodeValue[]::new);
            gen.writeObjectField("nodes", new ListValue(nodes));
            var relationships = StreamSupport.stream(path.relationships().spliterator(), false)
                    .map(RelationshipValue::new)
                    .toArray(RelationshipValue[]::new);
            gen.writeObjectField("relationships", new ListValue(relationships));
        });
    }
}
