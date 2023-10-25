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
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;

public class TestkitRelationshipValueSerializer extends StdSerializer<RelationshipValue> {
    @Serial
    private static final long serialVersionUID = 7011005216175127691L;

    public TestkitRelationshipValueSerializer() {
        super(RelationshipValue.class);
    }

    @Override
    @SuppressWarnings("deprecation")
    public void serialize(RelationshipValue relationshipValue, JsonGenerator gen, SerializerProvider provider)
            throws IOException {
        cypherObject(gen, "Relationship", () -> {
            var relationship = relationshipValue.asRelationship();
            gen.writeObjectField("id", new IntegerValue(getId(relationship::id)));
            gen.writeObjectField("elementId", new StringValue(relationship.elementId()));
            gen.writeObjectField("startNodeId", new IntegerValue(getId(relationship::startNodeId)));
            gen.writeObjectField("startNodeElementId", new StringValue(relationship.startNodeElementId()));
            gen.writeObjectField("endNodeId", new IntegerValue(getId(relationship::endNodeId)));
            gen.writeObjectField("endNodeElementId", new StringValue(relationship.endNodeElementId()));
            gen.writeObjectField("type", new StringValue(relationship.type()));
            gen.writeObjectField("props", new MapValue(relationship.asMap(Function.identity())));
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
