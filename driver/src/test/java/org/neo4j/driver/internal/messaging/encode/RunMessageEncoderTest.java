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
package org.neo4j.driver.internal.messaging.encode;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;

import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.DiscardAllMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.Value;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.neo4j.driver.Values.value;

class RunMessageEncoderTest
{
    private final RunMessageEncoder encoder = new RunMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @Test
    void shouldEncodeRunMessage() throws Exception
    {
        Map<String,Value> params = singletonMap( "answer", value( 42 ) );

        encoder.encode( new RunMessage( "RETURN $answer", params ), packer );

        InOrder order = inOrder( packer );
        order.verify( packer ).packStructHeader( 2, RunMessage.SIGNATURE );
        order.verify( packer ).pack( "RETURN $answer" );
        order.verify( packer ).pack( params );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( DiscardAllMessage.DISCARD_ALL, packer ) );
    }
}
