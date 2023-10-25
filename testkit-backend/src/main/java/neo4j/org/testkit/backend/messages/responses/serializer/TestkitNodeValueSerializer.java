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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.StringValue;

public class TestkitNodeValueSerializer extends StdSerializer<NodeValue> {
    @Serial
    private static final long serialVersionUID = 5456264010641357998L;

    public TestkitNodeValueSerializer() {
        super(NodeValue.class);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void serialize(NodeValue nodeValue, JsonGenerator gen, SerializerProvider serializerProvider)
            throws IOException {

        cypherObject(gen, "Node", () -> {
            var node = nodeValue.asNode();
            gen.writeObjectField("id", new IntegerValue(getId(node::id)));
            gen.writeObjectField("elementId", new StringValue(node.elementId()));

            var labels = StreamSupport.stream(node.labels().spliterator(), false)
                    .map(StringValue::new)
                    .toArray(StringValue[]::new);

            gen.writeObjectField("labels", new ListValue(labels));
            gen.writeObjectField("props", new MapValue(node.asMap(Function.identity())));
        });
    }

    private long getId(Supplier<Long> supplier) {
        try {
            return supplier.get();
        } catch (IllegalStateException e) {
            return -1;
        }
    }
}
