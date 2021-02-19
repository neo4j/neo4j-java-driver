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
package org.neo4j.driver.internal.summary;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

class InternalNotificationTest
{
    @Test
    void shouldHandleNotificationWithPosition()
    {
        // GIVEN
        Map<String,Value> map = new HashMap<>();
        map.put( "description", new StringValue( "A description" ) );
        map.put( "code", new StringValue( "Neo.DummyNotification" ) );
        map.put( "title", new StringValue( "A title" ) );
        map.put( "severity", new StringValue( "WARNING" ) );
        Map<String,Value> position = new HashMap<>();
        position.put( "offset", new IntegerValue( 0 ) );
        position.put( "column", new IntegerValue( 1 ) );
        position.put( "line", new IntegerValue( 2 ) );
        map.put( "position", new MapValue( position ) );
        Value value = new MapValue( map );

        // WHEN
        Notification notification = InternalNotification.VALUE_TO_NOTIFICATION.apply( value );

        // THEN
        assertThat( notification.description(), equalTo( "A description" ) );
        assertThat( notification.code(), equalTo( "Neo.DummyNotification" ) );
        assertThat( notification.title(), equalTo( "A title" ) );
        assertThat( notification.severity(), equalTo( "WARNING" ) );
        InputPosition pos = notification.position();
        assertThat( pos.offset(), equalTo( 0 ) );
        assertThat( pos.column(), equalTo( 1 ) );
        assertThat( pos.line(), equalTo( 2 ) );
    }

    @Test
    void shouldHandleNotificationWithoutPosition()
    {
        // GIVEN
        Map<String,Value> map = new HashMap<>();
        map.put( "description", new StringValue( "A description" ) );
        map.put( "code", new StringValue( "Neo.DummyNotification" ) );
        map.put( "title", new StringValue( "A title" ) );
        Value value = new MapValue( map );

        // WHEN
        Notification notification = InternalNotification.VALUE_TO_NOTIFICATION.apply( value );

        // THEN
        assertThat( notification.description(), equalTo( "A description" ) );
        assertThat( notification.code(), equalTo( "Neo.DummyNotification" ) );
        assertThat( notification.title(), equalTo( "A title" ) );
        assertThat( notification.position(), nullValue());
    }
}
