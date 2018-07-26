/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.driver.internal;

import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.TransactionConfig;

import static java.lang.System.lineSeparator;

class LeakLoggingNetworkSession extends NetworkSession
{
    private final String stackTrace;

    LeakLoggingNetworkSession( ConnectionProvider connectionProvider, AccessMode mode, RetryLogic retryLogic,
            TransactionConfig defaultTransactionConfig, Logging logging )
    {
        super( connectionProvider, mode, retryLogic, defaultTransactionConfig, logging );
        this.stackTrace = captureStackTrace();
    }

    @Override
    protected void finalize() throws Throwable
    {
        logLeakIfNeeded();
        super.finalize();
    }

    private void logLeakIfNeeded()
    {
        Boolean isOpen = Futures.blockingGet( currentConnectionIsOpen() );
        if ( isOpen )
        {
            logger.error( "Neo4j Session object leaked, please ensure that your application" +
                          "calls the `close` method on Sessions before disposing of the objects.\n" +
                          "Session was create at:\n" + stackTrace, null );
        }
    }
    private static String captureStackTrace()
    {
        StringBuilder result = new StringBuilder();
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for ( StackTraceElement element : elements )
        {
            result.append( "\t" ).append( element ).append( lineSeparator() );
        }
        return result.toString();
    }
}
