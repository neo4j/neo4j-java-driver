/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.docs.driver;

// tag::result-consume-import[]

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
// end::result-consume-import[]

public class ResultConsumeExample extends BaseApplication
{
    public ResultConsumeExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::result-consume[]
    public List<String> getPeople()
    {
        try ( Session session = driver.session() )
        {
            return session.readTransaction( new TransactionWork<List<String>>()
            {
                @Override
                public List<String> execute( Transaction tx )
                {
                    return matchPersonNodes( tx );
                }
            } );
        }
    }

    private static List<String> matchPersonNodes( Transaction tx )
    {
        List<String> names = new ArrayList<>();
        StatementResult result = tx.run( "MATCH (a:Person) RETURN a.name ORDER BY a.name" );
        while ( result.hasNext() )
        {
            names.add( result.next().get( 0 ).asString() );
        }
        return names;
    }
    // end::result-consume[]

}
