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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

import static org.neo4j.driver.internal.util.Futures.completedWithNull;

public class InternalDriver implements Driver
{
    private final SecurityPlan securityPlan;
    private final SessionFactory sessionFactory;
    private final Logger log;

    private AtomicBoolean closed = new AtomicBoolean( false );

    InternalDriver( SecurityPlan securityPlan, SessionFactory sessionFactory, Logging logging )
    {
        this.securityPlan = securityPlan;
        this.sessionFactory = sessionFactory;
        this.log = logging.getLog( Driver.class.getSimpleName() );
        log.info( "Driver instance %s created", this );
    }

    @Override
    public boolean isEncrypted()
    {
        assertOpen();
        return securityPlan.requiresEncryption();
    }

    @Override
    public Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public Session session( AccessMode mode )
    {
        return newSession( mode, Bookmark.empty() );
    }

    @Override
    public Session session( String bookmark )
    {
        return session( AccessMode.WRITE, bookmark );
    }

    @Override
    public Session session( AccessMode mode, String bookmark )
    {
        return newSession( mode, Bookmark.from( bookmark ) );
    }

    @Override
    public Session session( Iterable<String> bookmarks )
    {
        return session( AccessMode.WRITE, bookmarks );
    }

    @Override
    public Session session( AccessMode mode, Iterable<String> bookmarks )
    {
        return newSession( mode, Bookmark.from( bookmarks ) );
    }

    private Session newSession( AccessMode mode, Bookmark bookmark )
    {
        assertOpen();
        Session session = sessionFactory.newInstance( mode, bookmark );
        if ( closed.get() )
        {
            // session does not immediately acquire connection, it is fine to just throw
            throw driverCloseException();
        }
        return session;
    }

    @Override
    public void close()
    {
        Futures.blockingGet( closeAsync() );
    }

    @Override
    public CompletionStage<Void> closeAsync()
    {
        if ( closed.compareAndSet( false, true ) )
        {
            log.info( "Closing driver instance %s", this );
            return sessionFactory.close();
        }
        return completedWithNull();
    }

    public CompletionStage<Void> verifyConnectivity()
    {
        return sessionFactory.verifyConnectivity();
    }

    /**
     * Get the underlying session factory.
     * <p>
     * <b>This method is only for testing</b>
     *
     * @return the session factory used by this driver.
     */
    public SessionFactory getSessionFactory()
    {
        return sessionFactory;
    }

    private void assertOpen()
    {
        if ( closed.get() )
        {
            throw driverCloseException();
        }
    }

    private static RuntimeException driverCloseException()
    {
        return new IllegalStateException( "This driver instance has already been closed" );
    }
}
