/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.driver.internal.net;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.IgnoredMessage.IGNORED;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;

public class LoggingResponseHandler extends SocketResponseHandler
{
    private static final String DEFAULT_DEBUG_LOGGING_FORMAT = "S: %s";
    private final Logger logger;

    public LoggingResponseHandler( Logger logger )
    {
        this.logger = logger;
    }

    @Override
    public void handleInitMessage( String userAgent, Map<String,Value> authToken )
    {
        logger.debug( "S: INIT \"%s\" {...}", userAgent );
        super.handleInitMessage( userAgent, authToken );
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {
        logger.debug( "S: RUN \"%s\" %s", statement, parameters );
        super.handleRunMessage( statement, parameters );
    }

    @Override
    public void handlePullAllMessage()
    {
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, PULL_ALL );
        super.handlePullAllMessage();
    }

    @Override
    public void handleDiscardAllMessage()
    {
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, DISCARD_ALL );
        super.handleDiscardAllMessage();
    }

    @Override
    public void handleResetMessage()
    {
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, RESET );
        super.handleResetMessage();
    }

    @Override
    public void handleAckFailureMessage()
    {
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, ACK_FAILURE );
        super.handleAckFailureMessage();
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        logger.debug( "S: SUCCESS %s", meta );
        super.handleSuccessMessage( meta );
    }

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        logger.debug( "S: RECORD %s", Arrays.asList( fields ) );
        super.handleRecordMessage( fields );
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        logger.debug("S: FAILURE %s \"%s\"", code, message );
        super.handleFailureMessage( code, message );
    }

    @Override
    public void handleIgnoredMessage()
    {
        logger.debug( DEFAULT_DEBUG_LOGGING_FORMAT, IGNORED );
        super.handleIgnoredMessage();
    }
}
