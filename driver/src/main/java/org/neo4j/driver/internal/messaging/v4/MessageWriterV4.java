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
package org.neo4j.driver.internal.messaging.v4;

import java.util.Map;

import org.neo4j.driver.internal.messaging.AbstractMessageWriter;
import org.neo4j.driver.internal.messaging.MessageEncoder;
import org.neo4j.driver.internal.messaging.common.CommonValuePacker;
import org.neo4j.driver.internal.messaging.encode.BeginMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.CommitMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.DiscardMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.GoodbyeMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.HelloMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.PullMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.ResetMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.RollbackMessageEncoder;
import org.neo4j.driver.internal.messaging.encode.RunWithMetadataMessageEncoder;
import org.neo4j.driver.internal.messaging.request.BeginMessage;
import org.neo4j.driver.internal.messaging.request.CommitMessage;
import org.neo4j.driver.internal.messaging.request.DiscardMessage;
import org.neo4j.driver.internal.messaging.request.GoodbyeMessage;
import org.neo4j.driver.internal.messaging.request.HelloMessage;
import org.neo4j.driver.internal.messaging.request.PullMessage;
import org.neo4j.driver.internal.messaging.request.ResetMessage;
import org.neo4j.driver.internal.messaging.request.RollbackMessage;
import org.neo4j.driver.internal.messaging.request.RunWithMetadataMessage;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.util.Iterables;

public class MessageWriterV4 extends AbstractMessageWriter
{
    public MessageWriterV4( PackOutput output )
    {
        super( new CommonValuePacker( output ), buildEncoders() );
    }

    private static Map<Byte,MessageEncoder> buildEncoders()
    {
        Map<Byte,MessageEncoder> result = Iterables.newHashMapWithSize( 9 );
        result.put( HelloMessage.SIGNATURE, new HelloMessageEncoder() );
        result.put( GoodbyeMessage.SIGNATURE, new GoodbyeMessageEncoder() );
        result.put( RunWithMetadataMessage.SIGNATURE, new RunWithMetadataMessageEncoder() );

        result.put( DiscardMessage.SIGNATURE, new DiscardMessageEncoder() ); // new
        result.put( PullMessage.SIGNATURE, new PullMessageEncoder() ); // new

        result.put( BeginMessage.SIGNATURE, new BeginMessageEncoder() );
        result.put( CommitMessage.SIGNATURE, new CommitMessageEncoder() );
        result.put( RollbackMessage.SIGNATURE, new RollbackMessageEncoder() );

        result.put( ResetMessage.SIGNATURE, new ResetMessageEncoder() );
        return result;
    }
}
