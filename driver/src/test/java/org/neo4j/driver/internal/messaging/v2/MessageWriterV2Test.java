/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.driver.internal.messaging.v2;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import org.neo4j.driver.internal.util.messaging.AbstractMessageWriterTestBase;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat.Writer;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.InitMessage;
import org.neo4j.driver.internal.messaging.request.RunMessage;
import org.neo4j.driver.internal.packstream.PackOutput;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.neo4j.driver.internal.messaging.request.CommitMessage.COMMIT;
import static org.neo4j.driver.internal.messaging.request.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.request.GoodbyeMessage.GOODBYE;
import static org.neo4j.driver.internal.messaging.request.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.request.ResetMessage.RESET;
import static org.neo4j.driver.internal.messaging.request.RollbackMessage.ROLLBACK;
import static org.neo4j.driver.Values.point;
import static org.neo4j.driver.Values.value;

class MessageWriterV2Test extends AbstractMessageWriterTestBase
{
    @Override
    protected Writer newWriter( PackOutput output )
    {
        return new MessageWriterV2( output );
    }

    @Override
    protected Stream<Message> supportedMessages()
    {
        return Stream.of(
                new InitMessage( "MyDriver/1.2.3", singletonMap( "password", value( "hello" ) ) ),
                new RunMessage( "RETURN 1", singletonMap( "key", value( 42 ) ) ),
                new RunMessage( "RETURN $now", singletonMap( "now", value( LocalDateTime.now() ) ) ), // RUN with temporal value
                new RunMessage( "RETURN $here", singletonMap( "now", point( 42, 1, 1 ) ) ), // RUN with spatial value
                PULL_ALL,
                DISCARD_ALL,
                RESET
        );
    }

    @Override
    protected Stream<Message> unsupportedMessages()
    {
        return Stream.of(
                new HelloMessage( "JavaDriver/1.1.0", emptyMap() ),
                GOODBYE,
                COMMIT,
                ROLLBACK
        );
    }
}
