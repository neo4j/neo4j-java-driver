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

// tag::session-import[]

import org.neo4j.driver.Session;

import static org.neo4j.driver.Values.parameters;
// end::session-import[]

public class SessionExample extends BaseApplication
{
    public SessionExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::session[]
    public void addPerson(String name)
    {
        try ( Session session = driver.session() )
        {
            session.run("CREATE (a:Person {name: $name})", parameters( "name", name ) );
        }
    }
    // end::session[]
}
