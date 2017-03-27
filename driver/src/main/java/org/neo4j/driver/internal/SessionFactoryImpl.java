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
package org.neo4j.driver.internal;

import org.neo4j.driver.internal.retry.RetryLogic;
import org.neo4j.driver.internal.spi.ConnectionProvider;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

public class SessionFactoryImpl implements SessionFactory
{
    protected final ConnectionProvider connectionProvider;
    protected final RetryLogic retryLogic;
    protected final Logging logging;
    protected final boolean leakedSessionsLoggingEnabled;

    SessionFactoryImpl( ConnectionProvider connectionProvider, RetryLogic retryLogic, Config config )
    {
        this.connectionProvider = connectionProvider;
        this.leakedSessionsLoggingEnabled = config.logLeakedSessions();
        this.retryLogic = retryLogic;
        this.logging = config.logging();
    }

    @Override
    public Session newInstance( AccessMode mode, String bookmark )
    {
        NetworkSession session;
        if ( leakedSessionsLoggingEnabled )
        {
            session = new LeakLoggingNetworkSession( connectionProvider, mode, retryLogic, logging );
        }
        else
        {
            session = new NetworkSession( connectionProvider, mode, retryLogic, logging );
        }
        session.setBookmark( bookmark );
        return session;
    }

    @Override
    public void close() throws Exception
    {
        connectionProvider.close();
    }

    /**
     * Get the underlying connection provider.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the connection provider used by this factory.
     */
    public ConnectionProvider getConnectionProvider()
    {
        return connectionProvider;
    }
}
