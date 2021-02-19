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

import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.messaging.request.CommitMessage;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;

class CommitMessageEncoderTest
{
    private final CommitMessageEncoder encoder = new CommitMessageEncoder();
    private final ValuePacker packer = mock( ValuePacker.class );

    @Test
    void shouldEncodeCommitMessage() throws Exception
    {
        encoder.encode( COMMIT, packer );

        verify( packer ).packStructHeader( 0, CommitMessage.SIGNATURE );
    }

    @Test
    void shouldFailToEncodeWrongMessage()
    {
        assertThrows( IllegalArgumentException.class, () -> encoder.encode( DISCARD_ALL, packer ) );
    }
}
