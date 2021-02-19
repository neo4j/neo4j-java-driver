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

// tag::result-consume-import[]

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
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
            return session.readTransaction( tx -> {
                List<String> names = new ArrayList<>();
                Result result = tx.run( "MATCH (a:Person) RETURN a.name ORDER BY a.name" );
                while ( result.hasNext() )
                {
                    names.add( result.next().get( 0 ).asString() );
                }
                return names;
            } );
        }
    }
    // end::result-consume[]
}
