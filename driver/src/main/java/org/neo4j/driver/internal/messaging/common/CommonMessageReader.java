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
package org.neo4j.driver.internal.messaging.common;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.ResponseMessageHandler;
import org.neo4j.driver.internal.messaging.ValueUnpacker;
import org.neo4j.driver.internal.messaging.response.FailureMessage;
import org.neo4j.driver.internal.messaging.response.IgnoredMessage;
import org.neo4j.driver.internal.messaging.response.RecordMessage;
import org.neo4j.driver.internal.messaging.response.SuccessMessage;
import org.neo4j.driver.internal.packstream.PackInput;

public class CommonMessageReader implements MessageFormat.Reader
{
    private final ValueUnpacker unpacker;

    public CommonMessageReader( PackInput input )
    {
        this( new CommonValueUnpacker( input ) );
    }

    protected CommonMessageReader( ValueUnpacker unpacker )
    {
        this.unpacker = unpacker;
    }

    @Override
    public void read( ResponseMessageHandler handler ) throws IOException
    {
        unpacker.unpackStructHeader();
        int type = unpacker.unpackStructSignature();
        switch ( type )
        {
        case SuccessMessage.SIGNATURE:
            unpackSuccessMessage( handler );
            break;
        case FailureMessage.SIGNATURE:
            unpackFailureMessage( handler );
            break;
        case IgnoredMessage.SIGNATURE:
            unpackIgnoredMessage( handler );
            break;
        case RecordMessage.SIGNATURE:
            unpackRecordMessage( handler );
            break;
        default:
            throw new IOException( "Unknown message type: " + type );
        }
    }

    private void unpackSuccessMessage( ResponseMessageHandler output ) throws IOException
    {
        Map<String,Value> map = unpacker.unpackMap();
        output.handleSuccessMessage( map );
    }

    private void unpackFailureMessage( ResponseMessageHandler output ) throws IOException
    {
        Map<String,Value> params = unpacker.unpackMap();
        String code = params.get( "code" ).asString();
        String message = params.get( "message" ).asString();
        output.handleFailureMessage( code, message );
    }

    private void unpackIgnoredMessage( ResponseMessageHandler output ) throws IOException
    {
        output.handleIgnoredMessage();
    }

    private void unpackRecordMessage( ResponseMessageHandler output ) throws IOException
    {
        Value[] fields = unpacker.unpackArray();
        output.handleRecordMessage( fields );
    }
}
