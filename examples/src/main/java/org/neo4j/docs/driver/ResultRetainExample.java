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

// tag::result-retain-import[]

import java.util.List;

import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;

import static org.neo4j.driver.Values.parameters;
// end::result-retain-import[]

public class ResultRetainExample extends BaseApplication
{
    public ResultRetainExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::result-retain[]
    public int addEmployees( final String companyName )
    {
        try ( Session session = driver.session() )
        {
            int employees = 0;
            List<Record> persons = session.readTransaction( new TransactionWork<List<Record>>()
            {
                @Override
                public List<Record> execute( Transaction tx )
                {
                    return matchPersonNodes( tx );
                }
            } );
            for ( final Record person : persons )
            {
                employees += session.writeTransaction( new TransactionWork<Integer>()
                {
                    @Override
                    public Integer execute( Transaction tx )
                    {
                        tx.run( "MATCH (emp:Person {name: $person_name}) " +
                                "MERGE (com:Company {name: $company_name}) " +
                                "MERGE (emp)-[:WORKS_FOR]->(com)",
                                parameters( "person_name", person.get( "name" ).asString(), "company_name",
                                        companyName ) );
                        return 1;
                    }
                } );
            }
            return employees;
        }
    }

    private static List<Record> matchPersonNodes( Transaction tx )
    {
        return tx.run( "MATCH (a:Person) RETURN a.name AS name" ).list();
    }

    // end::result-retain[]

}
