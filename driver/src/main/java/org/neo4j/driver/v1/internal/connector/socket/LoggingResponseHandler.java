/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.driver.v1.internal.connector.socket;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.internal.spi.Logger;

import static org.neo4j.driver.v1.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.v1.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.v1.internal.messaging.IgnoredMessage.IGNORED;
import static org.neo4j.driver.v1.internal.messaging.PullAllMessage.PULL_ALL;

public class LoggingResponseHandler extends SocketResponseHandler
{
    private static final String DEFAULT_DEBUG_LOGGING_FORMAT = "S: %s";
    private final Logger logger;

    public LoggingResponseHandler( Logger logger )
    {
        this.logger = logger;
    }

    @Override
    public void handleInitMessage( String clientNameAndVersion )
    {
        super.handleInitMessage( clientNameAndVersion );
        logger.debug( "S: [INIT \"%s\"]", clientNameAndVersion );
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {
        super.handleRunMessage( statement, parameters );
        logger.debug( "S: [RUN \"%s\" %s]", statement, parameters );
    }

    @Override
    public void handlePullAllMessage()
    {
        super.handlePullAllMessage();
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, PULL_ALL );
    }

    @Override
    public void handleDiscardAllMessage()
    {
        super.handleDiscardAllMessage();
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, DISCARD_ALL );
    }

    @Override
    public void handleAckFailureMessage()
    {
        super.handleAckFailureMessage();
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, ACK_FAILURE );
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        super.handleSuccessMessage( meta );
        logger.debug( "S: [SUCCESS %s]", meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        super.handleRecordMessage( fields );
        logger.debug( "S: RecordMessage{%s}", Arrays.asList( fields ) );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        super.handleFailureMessage( code, message );
        logger.debug("S: [FAILURE %s \"%s\"]", code, message );
    }

    @Override
    public void handleIgnoredMessage()
    {
        super.handleIgnoredMessage();
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, IGNORED );
    }
}
