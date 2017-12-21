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

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.driver.internal.security.SecurityPlan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.logging.DevNullLogging.DEV_NULL_LOGGING;
import static org.neo4j.driver.internal.util.Futures.completedWithNull;
import static org.neo4j.driver.v1.util.TestUtil.await;

public class InternalDriverTest
{
    @Test
    public void shouldCloseSessionFactory()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver( sessionFactory );

        assertNull( await( driver.closeAsync() ) );
        verify( sessionFactory ).close();
    }

    @Test
    public void shouldNotCloseSessionFactoryMultipleTimes()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        InternalDriver driver = newDriver( sessionFactory );

        assertNull( await( driver.closeAsync() ) );
        assertNull( await( driver.closeAsync() ) );
        assertNull( await( driver.closeAsync() ) );

        verify( sessionFactory ).close();
    }

    @Test
    public void shouldVerifyConnectivity()
    {
        SessionFactory sessionFactory = sessionFactoryMock();
        CompletableFuture<Void> connectivityStage = completedWithNull();
        when( sessionFactory.verifyConnectivity() ).thenReturn( connectivityStage );

        InternalDriver driver = newDriver( sessionFactory );

        assertEquals( connectivityStage, driver.verifyConnectivity() );
    }

    private static InternalDriver newDriver( SessionFactory sessionFactory )
    {
        return new InternalDriver( SecurityPlan.insecure(), sessionFactory, DEV_NULL_LOGGING );
    }

    private static SessionFactory sessionFactoryMock()
    {
        SessionFactory sessionFactory = mock( SessionFactory.class );
        when( sessionFactory.close() ).thenReturn( completedWithNull() );
        return sessionFactory;
    }
}
