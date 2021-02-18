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

// tag::cypher-error-import[]

import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.exceptions.ClientException;

import static org.neo4j.driver.Values.parameters;
// end::cypher-error-import[]

public class CypherErrorExample extends BaseApplication
{
    public CypherErrorExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::cypher-error[]
    public int getEmployeeNumber( final String name )
    {
        try ( Session session = driver.session() )
        {
            return session.readTransaction( new TransactionWork<Integer>()
            {
                @Override
                public Integer execute( Transaction tx )
                {
                    return selectEmployee( tx, name );
                }
            } );
        }
    }

    private static int selectEmployee( Transaction tx, String name )
    {
        try
        {
            Result result = tx.run( "SELECT * FROM Employees WHERE name = $name", parameters( "name", name ) );
            return result.single().get( "employee_number" ).asInt();
        }
        catch ( ClientException ex )
        {
            tx.rollback();
            System.err.println( ex.getMessage() );
            return -1;
        }
    }
    // end::cypher-error[]

}
