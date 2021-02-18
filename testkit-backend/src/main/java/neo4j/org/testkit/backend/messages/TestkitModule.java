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
package neo4j.org.testkit.backend.messages;

import com.fasterxml.jackson.databind.module.SimpleModule;
import neo4j.org.testkit.backend.messages.requests.deserializer.TestkitListDeserializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitBookmarkSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitListValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitMapValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitNodeValueSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitRecordSerializer;
import neo4j.org.testkit.backend.messages.responses.serializer.TestkitValueSerializer;

import java.util.List;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;

public class TestkitModule extends SimpleModule
{
    public TestkitModule()
    {
        this.addDeserializer( List.class, new TestkitListDeserializer() );

        this.addSerializer( Value.class, new TestkitValueSerializer() );
        this.addSerializer( NodeValue.class, new TestkitNodeValueSerializer() );
        this.addSerializer( ListValue.class, new TestkitListValueSerializer() );
        this.addSerializer( Record.class, new TestkitRecordSerializer() );
        this.addSerializer( MapValue.class, new TestkitMapValueSerializer() );
        this.addSerializer( Bookmark.class, new TestkitBookmarkSerializer() );
    }
}
