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
package org.neo4j.driver.v1;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.driver.v1.internal.SimpleIdentity;
import org.neo4j.driver.v1.internal.SimpleNode;
import org.neo4j.driver.v1.internal.SimplePath;
import org.neo4j.driver.v1.internal.SimpleRelationship;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.CypherTypes.*;
import static org.neo4j.driver.v1.CypherTypes.ANY;
import static org.neo4j.driver.v1.CypherTypes.BOOLEAN;
import static org.neo4j.driver.v1.CypherTypes.FLOAT;
import static org.neo4j.driver.v1.CypherTypes.INTEGER;
import static org.neo4j.driver.v1.CypherTypes.LIST;
import static org.neo4j.driver.v1.CypherTypes.NODE;
import static org.neo4j.driver.v1.CypherTypes.PATH;
import static org.neo4j.driver.v1.CypherTypes.RELATIONSHIP;
import static org.neo4j.driver.v1.CypherTypes.STRING;
import static org.neo4j.driver.v1.CypherTypes.typeOf;
import static org.neo4j.driver.v1.Values.value;

public class CypherTypesTest
{
    private final SimpleNode node = new SimpleNode( 42L );
    private final SimpleRelationship relationship = new SimpleRelationship( 42L, 42L, 43L, "T" );

    private Value integerValue = value( 13 );
    private Value floatValue = value( 13.1 );
    private Value stringValue = value( "Lalala " );
    private Value nodeValue = value( node );
    private Value relationshipValue = value( relationship );
    private Value mapValue = value( Collections.singletonMap( "type", "r" ) );
    private Value pathValue = value( new SimplePath( Arrays.<Entity>asList( node, relationship, node ) ) );
    private Value booleanValue = value( true );
    private Value listValue = value( Arrays.asList( 1, 2, 3 ) );
    private Value nullValue = null;
    private Value identityValue = value( new SimpleIdentity( 42L ) );

    TypeVerifier newTypeVerifierFor( CoarseCypherType type )
    {
        HashSet<Value> allValues = new HashSet<>();
        allValues.add( integerValue );
        allValues.add( stringValue );
        allValues.add( floatValue );
        allValues.add( nodeValue );
        allValues.add( relationshipValue );
        allValues.add( mapValue );
        allValues.add( pathValue );
        allValues.add( booleanValue );
        allValues.add( nullValue );
        allValues.add( listValue );
        allValues.add( identityValue );
        return new TypeVerifier( type, allValues );
    }

    @Test
    public void shouldNameTypeCorrectly()
    {
        assertThat( ANY.name(), is( "ANY" ) );
        assertThat( BOOLEAN.name(), is( "BOOLEAN" ) );
        assertThat( STRING.name(), is( "STRING" ) );
        assertThat( NUMBER.name(), is( "NUMBER" ) );
        assertThat( INTEGER.name(), is( "INTEGER" ) );
        assertThat( FLOAT.name(), is( "FLOAT" ) );
        assertThat( LIST.name(), is( "LIST OF ANY?" ) );
        assertThat( MAP.name(), is( "MAP" ) );
        assertThat( IDENTITY.name(), is( "IDENTITY" ) );
        assertThat( NODE.name(), is( "NODE" ) );
        assertThat( RELATIONSHIP.name(), is( "RELATIONSHIP" ) );
        assertThat( PATH.name(), is( "PATH" ) );
        assertThat( NULL.name(), is( "NULL" ) );
    }

    @Test
    public void shouldInferAnyTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( ANY ) )
        {
            verifier.assertIncludes( booleanValue );
            verifier.assertIncludes( stringValue );
            verifier.assertIncludes( integerValue );
            verifier.assertIncludes( floatValue );
            verifier.assertIncludes( listValue );
            verifier.assertIncludes( mapValue );
            verifier.assertIncludes( identityValue );
            verifier.assertIncludes( nodeValue );
            verifier.assertIncludes( relationshipValue );
            verifier.assertIncludes( pathValue );
        }
    }

    @Test
    public void shouldInferNumberTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( NUMBER ) )
        {
            verifier.assertIncludes( integerValue );
            verifier.assertIncludes( floatValue );
        }
    }

    @Test
    public void shouldInferNodesTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( NODE ) )
        {
            verifier.assertIncludes( nodeValue );
        }
    }

    @Test
    public void shouldInferRelTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( RELATIONSHIP ) )
        {
            verifier.assertIncludes( relationshipValue );
        }
    }

    @Test
    public void shouldInferStringTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( STRING ) )
        {
            verifier.assertIncludes( stringValue );
        }
    }

    @Test
    public void shouldInferMapTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( MAP ) )
        {
            verifier.assertIncludes( nodeValue );
            verifier.assertIncludes( relationshipValue );
            verifier.assertIncludes( mapValue );
        }
    }

    @Test
    public void shouldInferPathTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( PATH ) )
        {
            verifier.assertIncludes( pathValue );
        }
    }

    @Test
    public void shouldInferNullCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( NULL ) )
        {
            verifier.assertIncludes( nullValue );
        }
    }

    @Test
    public void shouldInferBooleanTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( BOOLEAN ) )
        {
            verifier.assertIncludes( booleanValue );
        }
    }

    @Test
    public void shouldIntegerTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( INTEGER ) )
        {
            verifier.assertIncludes( integerValue );
        }
    }

    @Test
    public void shouldInferFloatTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( FLOAT ) )
        {
            verifier.assertIncludes( floatValue );
        }
    }

    @Test
    public void shouldInferListTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( LIST ) )
        {
            verifier.assertIncludes( listValue );
        }
    }

    @Test
    public void shouldInferIdentityTypeCorrectly() {
        try ( TypeVerifier verifier = newTypeVerifierFor( IDENTITY ) )
        {
            verifier.assertIncludes( identityValue );
        }
    }

    @Test
    public void shouldDetermineTypeCorrectly()
    {
        assertThat( typeOf( integerValue ), equalTo( INTEGER ) );
        assertThat( typeOf( floatValue ), equalTo( FLOAT ) );
        assertThat( typeOf( stringValue ), equalTo( STRING ) );
        assertThat( typeOf( booleanValue ), equalTo( BOOLEAN ) );
        assertThat( typeOf( listValue ), equalTo( LIST ) );
        assertThat( typeOf( mapValue ), equalTo( MAP ) );
        assertThat( typeOf( identityValue ), equalTo( IDENTITY ) );
        assertThat( typeOf( nodeValue ), equalTo( NODE ) );
        assertThat( typeOf( relationshipValue ), equalTo( RELATIONSHIP ) );
        assertThat( typeOf( pathValue ), equalTo( PATH ) );
        assertThat( typeOf( nullValue ), equalTo( NULL ) );
    }

    private class TypeVerifier implements AutoCloseable
    {
        private final CoarseCypherType type;
        private final Set<Value> values;

        TypeVerifier( CoarseCypherType type, Set<Value> values )
        {
            this.type = type;
            this.values = values;
        }

        public void assertIncludes( Value value )
        {
            assertThat( value, hasType( type ) );
            values.remove( value );
        }

        @Override
        public void close()
        {
            for ( Value value : values )
            {
                assertThat( value, not(hasType( type )) );
            }
        }
    }

    private Matcher<? super Value> hasType( final CoarseCypherType type )
    {
        return new BaseMatcher<Value>()
        {
            @Override
            public boolean matches( Object o )
            {
                return (o instanceof Value || o == null) && type.isTypeOf( (Value) o );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( type.name() );
            }
        };
    }

}
