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
package org.neo4j.docs.driver;

// tag::service-unavailable-import[]

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ServiceUnavailableException;

import static java.util.concurrent.TimeUnit.SECONDS;
// end::service-unavailable-import[]

public class ServiceUnavailableExample implements AutoCloseable
{
    protected final Driver driver;

    public ServiceUnavailableExample( String uri, String user, String password )
    {
        Config config = Config.builder()
                .withMaxTransactionRetryTime( 3, SECONDS )
                .withLogging( Logging.none() )
                .build();

        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), config );
    }

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    // tag::service-unavailable[]
    public boolean addItem()
    {
        try ( Session session = driver.session() )
        {
            return session.writeTransaction( new TransactionWork<Boolean>()
            {
                @Override
                public Boolean execute( Transaction tx )
                {
                    tx.run( "CREATE (a:Item)" );
                    return true;
                }
            } );
        }
        catch ( ServiceUnavailableException ex )
        {
            return false;
        }
    }
    // end::service-unavailable[]

}
