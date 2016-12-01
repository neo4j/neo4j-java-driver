/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Session;

abstract class BaseDriver implements Driver
{
    private final static String DRIVER_LOG_NAME = "Driver";

    private final SecurityPlan securityPlan;
    protected final Logger log;

    private final ReentrantReadWriteLock closedLock = new ReentrantReadWriteLock();
    private boolean closed;

    BaseDriver( SecurityPlan securityPlan, Logging logging )
    {
        this.securityPlan = securityPlan;
        this.log = logging.getLog( DRIVER_LOG_NAME );
    }

    @Override
    public final boolean isEncrypted()
    {
        closedLock.readLock().lock();
        try
        {
            assertOpen();
            return securityPlan.requiresEncryption();
        }
        finally
        {
            closedLock.readLock().unlock();
        }
    }

    @Override
    public final Session session()
    {
        return session( AccessMode.WRITE );
    }

    @Override
    public final Session session( AccessMode mode )
    {
        closedLock.readLock().lock();
        try
        {
            assertOpen();
            return newSessionWithMode( mode );
        }
        finally
        {
            closedLock.readLock().unlock();
        }
    }

    @Override
    public final void close()
    {
        closedLock.writeLock().lock();
        try
        {
            if ( !closed )
            {
                closeResources();
            }
        }
        finally
        {
            closed = true;
            closedLock.writeLock().unlock();
        }
    }

    protected abstract Session newSessionWithMode( AccessMode mode );

    protected abstract void closeResources();

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This driver instance has already been closed" );
        }
    }
}
