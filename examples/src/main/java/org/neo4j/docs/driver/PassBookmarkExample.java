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

// tag::pass-bookmarks-import[]
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Bookmark;

import static org.neo4j.driver.Values.parameters;
import static org.neo4j.driver.SessionConfig.builder;
// end::pass-bookmarks-import[]

public class PassBookmarkExample extends BaseApplication
{

    public PassBookmarkExample( String uri, String user, String password )
    {
        super( uri, user, password );
    }

    // tag::pass-bookmarks[]
    // Create a company node
    private Result addCompany(final Transaction tx, final String name )
    {
        return tx.run( "CREATE (:Company {name: $name})", parameters( "name", name ) );
    }

    // Create a person node
    private Result addPerson(final Transaction tx, final String name )
    {
        return tx.run( "CREATE (:Person {name: $name})", parameters( "name", name ) );
    }

    // Create an employment relationship to a pre-existing company node.
    // This relies on the person first having been created.
    private Result employ(final Transaction tx, final String person, final String company )
    {
        return tx.run( "MATCH (person:Person {name: $person_name}) " +
                        "MATCH (company:Company {name: $company_name}) " +
                        "CREATE (person)-[:WORKS_FOR]->(company)",
                parameters( "person_name", person, "company_name", company ) );
    }

    // Create a friendship between two people.
    private Result makeFriends(final Transaction tx, final String person1, final String person2 )
    {
        return tx.run( "MATCH (a:Person {name: $person_1}) " +
                        "MATCH (b:Person {name: $person_2}) " +
                        "MERGE (a)-[:KNOWS]->(b)",
                parameters( "person_1", person1, "person_2", person2 ) );
    }

    // Match and display all friendships.
    private Result printFriends(final Transaction tx )
    {
        Result result = tx.run( "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name" );
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( String.format( "%s knows %s", record.get( "a.name" ).asString(), record.get( "b.name" ).toString() ) );
        }
        return result;
    }

    public void addEmployAndMakeFriends()
    {
        // To collect the session bookmarks
        List<Bookmark> savedBookmarks = new ArrayList<>();

        // Create the first person and employment relationship.
        try ( Session session1 = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            session1.writeTransaction( tx -> addCompany( tx, "Wayne Enterprises" ) );
            session1.writeTransaction( tx -> addPerson( tx, "Alice" ) );
            session1.writeTransaction( tx -> employ( tx, "Alice", "Wayne Enterprises" ) );

            savedBookmarks.add( session1.lastBookmark() );
        }

        // Create the second person and employment relationship.
        try ( Session session2 = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).build() ) )
        {
            session2.writeTransaction( tx -> addCompany( tx, "LexCorp" ) );
            session2.writeTransaction( tx -> addPerson( tx, "Bob" ) );
            session2.writeTransaction( tx -> employ( tx, "Bob", "LexCorp" ) );

            savedBookmarks.add( session2.lastBookmark() );
        }

        // Create a friendship between the two people created above.
        try ( Session session3 = driver.session( builder().withDefaultAccessMode( AccessMode.WRITE ).withBookmarks( savedBookmarks ).build() ) )
        {
            session3.writeTransaction( tx -> makeFriends( tx, "Alice", "Bob" ) );

            session3.readTransaction( this::printFriends );
        }
    }
    // end::pass-bookmarks[]
}
