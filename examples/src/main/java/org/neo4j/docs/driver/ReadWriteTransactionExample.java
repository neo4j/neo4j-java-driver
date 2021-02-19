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

// tag::read-write-transaction-import[]

import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import static org.neo4j.driver.Values.parameters;
// end::read-write-transaction-import[]

public class ReadWriteTransactionExample extends BaseApplication
{
    public ReadWriteTransactionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::read-write-transaction[]
    public long addPerson( final String name )
    {
        try ( Session session = driver.session() )
        {
            session.writeTransaction( new TransactionWork<Void>()
            {
                @Override
                public Void execute( Transaction tx )
                {
                    return createPersonNode( tx, name );
                }
            } );
            return session.readTransaction( new TransactionWork<Long>()
            {
                @Override
                public Long execute( Transaction tx )
                {
                    return matchPersonNode( tx, name );
                }
            } );
        }
    }

    private static Void createPersonNode( Transaction tx, String name )
    {
        tx.run( "CREATE (a:Person {name: $name})", parameters( "name", name ) );
        return null;
    }

    private static long matchPersonNode( Transaction tx, String name )
    {
        Result result = tx.run( "MATCH (a:Person {name: $name}) RETURN id(a)", parameters( "name", name ) );
        return result.single().get( 0 ).asLong();
    }
    // end::read-write-transaction[]

}
