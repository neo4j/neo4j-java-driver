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
package neo4j.org.testkit.backend.messages.responses.serializer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Node;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherObject;

public class TestkitNodeValueSerializer extends StdSerializer<NodeValue>
{
    public TestkitNodeValueSerializer()
    {
        super( NodeValue.class );
    }

    @Override
    public void serialize( NodeValue nodeValue, JsonGenerator gen, SerializerProvider serializerProvider ) throws IOException
    {

        cypherObject( gen, "Node", () ->
        {
            Node node = nodeValue.asNode();
            gen.writeObjectField( "id", node.id() );

            StringValue[] labels = StreamSupport.stream( node.labels().spliterator(), false )
                                                .map( StringValue::new )
                                                .toArray( StringValue[]::new );

            gen.writeObjectField( "labels", new ListValue( labels ) );
            gen.writeObjectField( "props", new MapValue( node.asMap( Function.identity() ) ) );

        } );
    }
}
