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
package org.neo4j.driver.internal.util.messaging;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.driver.internal.async.inbound.InboundMessageDispatcher;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Value;

public class MemorizingInboundMessageDispatcher extends InboundMessageDispatcher
{
    private final List<Message> messages = new CopyOnWriteArrayList<>();

    public MemorizingInboundMessageDispatcher( Channel channel, Logging logging )
    {
        super( channel, logging );
    }

    public List<Message> messages()
    {
        return new ArrayList<>( messages );
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        messages.add( new SuccessMessage( meta ) );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        messages.add( new RecordMessage( fields ) );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        messages.add( new FailureMessage( code, message ) );
    }

    @Override
    public void handleIgnoredMessage()
    {
        messages.add( IgnoredMessage.IGNORED );
    }
}
