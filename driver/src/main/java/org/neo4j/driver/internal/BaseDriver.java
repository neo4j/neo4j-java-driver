/**
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

import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.RetryLogic;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.NotCommittedException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.util.Function;

import static java.lang.String.format;

import static org.neo4j.driver.v1.RetryLogic.TRY_UP_TO_3_TIMES_WITH_5_SECOND_PAUSE;

abstract class BaseDriver implements Driver
{
    private final DriverContract contract;
    private final SecurityPlan securityPlan;
    protected final Logger log;
    private final static String DRIVER_LOG_NAME = "Driver";

    BaseDriver( DriverContract contract, SecurityPlan securityPlan, Logging logging )
    {
        this.contract = contract;
        this.securityPlan = securityPlan;
        this.log = logging.getLog( DRIVER_LOG_NAME );
    }

    @Override
    public boolean isEncrypted()
    {
        return securityPlan.requiresEncryption();
    }

    public <T> T transact( RetryLogic logic, AccessMode mode, Function<Transaction, T> work )
            throws NotCommittedException, ServiceUnavailableException
    {
        int remaining = logic.attempts();
        while ( remaining > 0 )
        {
            try ( Session session = session( mode ) )
            {
                Transaction tx = session.beginTransaction();
                try {
                    T result = work.apply( tx );
                    tx.success();
                    return result;
                }
                catch ( SessionExpiredException e )
                {
                    tx.failure();
                    remaining -= 1;
                }
                finally
                {
                    tx.close();
                }
            }
            try
            {
                Thread.sleep( logic.pause() );
            }
            catch ( InterruptedException e )
            {
                throw new NotCommittedException( format( "Interrupted after %d attempts", logic.attempts() - remaining ) );
            }
        }
        throw new NotCommittedException( format( "Unable to commit transaction after %d attempts", logic.attempts() ) );
    }

    @Override
    public <T> T read( Function<Transaction, T> work ) throws NotCommittedException, ServiceUnavailableException
    {
        return transact( contract.retryLogic(), AccessMode.READ, work );
    }

    @Override
    public <T> T write( Function<Transaction, T> work ) throws NotCommittedException, ServiceUnavailableException
    {
        return transact( contract.retryLogic(), AccessMode.WRITE, work );
    }
}
