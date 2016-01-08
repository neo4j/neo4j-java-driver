/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

// NOTE: Be careful about auto-formatting here: All imports should be between the tags below.
// tag::minimal-example-import[]
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Session;
// end::minimal-example-import[]

public class MinimalWorkingExample
{
    public static void minimalWorkingExample() throws Exception
    {
        // tag::minimal-example[]
        Driver driver = GraphDatabase.driver( "bolt://localhost" );
        Session session = driver.session();

        session.run( "CREATE (neo:Person {name:'Neo', age:23})" );

        ResultCursor result = session.run( "MATCH (p:Person) WHERE p.name = 'Neo' RETURN p.age" );
        while ( result.next() )
        {
            System.out.println( "Neo is " + result.value( "p.age" ).asInt() + " years old." );
        }

        session.close();
        driver.close();
        // end::minimal-example[]
    }
}
