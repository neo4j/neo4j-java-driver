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

// tag::database-selection-import[]
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
// end::database-selection-import[]

public class DatabaseSelectionExample extends BaseApplication
{
    public DatabaseSelectionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    public void useAnotherDatabaseExample()
    {
        // tag::database-selection[]
        try ( Session session = driver.session( SessionConfig.forDatabase( "examples" ) ) )
        {
            session.run( "CREATE (a:Greeting {message: 'Hello, Example-Database'}) RETURN a" ).consume();
        }

        SessionConfig sessionConfig = SessionConfig.builder()
                .withDatabase( "examples" )
                .withDefaultAccessMode( AccessMode.READ )
                .build();
        try ( Session session = driver.session( sessionConfig ) )
        {
            String msg = session.run( "MATCH (a:Greeting) RETURN a.message as msg" ).single().get( "msg" ).asString();
            System.out.println(msg);
        }
        // end::database-selection[]
    }
}
