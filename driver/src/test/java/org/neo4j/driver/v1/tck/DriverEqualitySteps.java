/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1.tck;

import cucumber.api.java.en.And;
import cucumber.api.java.en.Then;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.v1.Entity;
import org.neo4j.driver.v1.Identity;
import org.neo4j.driver.v1.Node;
import org.neo4j.driver.v1.Pair;
import org.neo4j.driver.v1.Path;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Relationship;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.driver.v1.tck.Environment.driver;

public class DriverEqualitySteps
{
    HashMap<String,Value> savedValues = new HashMap<>();

    @And( "^`(.*)` is single value result of: (.*)$" )
    public void valueIsSingleValueResultOfMATCHNLabelRETURNN( String key, String statement ) throws Throwable
    {
        try ( Session session = driver.session())
        {
            Record r = session.run( statement ).single();
            assertThat( r.size(), equalTo( 1 ) );
            savedValues.put( key, r.get( 0 ) );
        }
    }


    @Then( "^saved values should all equal$" )
    public void savedValuesShouldAllEqual() throws Throwable
    {
        assertTrue( savedValues.values().size() > 1 );
        Collection values = savedValues.values();
        Iterator<Value> itr = values.iterator();
        Value v = itr.next();
        while ( itr.hasNext() )
        {
            assertThat( v, equalTo( itr.next() ) );
        }
    }

    @Then( "^none of the saved values should be equal$" )
    public void noneOfTheSavedValuesShouldBeEqual() throws Throwable
    {
        assertTrue( savedValues.values().size() > 1 );
        Collection values = savedValues.values();
        Iterator<Value> itr = values.iterator();
        Value v = itr.next();
        while ( itr.hasNext() )
        {
            assertThat( v, not(equalTo( itr.next() ) ) );
        }
    }

    @And( "^`(.*)` is a copy of `(.*)` path with flipped relationship direction$" )
    public void valueIsACopyOfValuePathWithFlippedRelationshipDirection( String key1, String key2 ) throws
            Throwable
    {
        Path path = savedValues.get( key2 ).asPath();
        Iterable<Relationship> relationships = path.relationships();
        Iterator<Node> nodes= path.nodes().iterator();

        ArrayList<Entity> entities = new ArrayList<>( );
        entities.add( nodes.next() );

        for ( Relationship rel : relationships )
        {
            Identity id = rel.identity();
            Identity end = rel.start();
            Identity start = rel.end();
            HashMap<String,Value> properties = new HashMap<>();
            for ( Pair<String,Value> pair : rel.properties() )
            {
                properties.put( pair.key(), pair.value() );
            }
            entities.add(
                    new InternalRelationship(
                            id.asLong(),
                            start.asLong(),
                            end.asLong(),
                            rel.type(),
                            properties ) );
            entities.add( nodes.next() );
        }
        Path newPath = new InternalPath( entities );
        savedValues.put( key1, new PathValue( newPath) );
    }
}
