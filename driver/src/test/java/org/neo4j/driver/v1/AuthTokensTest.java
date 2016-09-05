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
package org.neo4j.driver.v1;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.StringValue;

import static java.util.Arrays.asList;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.driver.v1.AuthTokens.basic;
import static org.neo4j.driver.v1.AuthTokens.custom;
import static org.neo4j.driver.v1.Values.values;

public class AuthTokensTest
{

    @Test
    public void basicAuthWithoutRealm()
    {
        InternalAuthToken basic = (InternalAuthToken) basic( "foo", "bar" );

        Map<String,Value> map = basic.toMap();

        assertThat( map.size(), equalTo( 3 ) );
        assertThat( map.get( "scheme" ), equalTo( (Value) new StringValue( "basic" ) ) );
        assertThat( map.get( "principal" ), equalTo( (Value) new StringValue( "foo" ) ) );
        assertThat( map.get( "credentials" ), equalTo( (Value) new StringValue( "bar" ) ) );
    }

    @Test
    public void basicAuthWithRealm()
    {
        InternalAuthToken basic = (InternalAuthToken) basic( "foo", "bar", "baz" );

        Map<String,Value> map = basic.toMap();

        assertThat( map.size(), equalTo( 4 ) );
        assertThat( map.get( "scheme" ), equalTo( (Value) new StringValue( "basic" ) ) );
        assertThat( map.get( "principal" ), equalTo( (Value) new StringValue( "foo" ) ) );
        assertThat( map.get( "credentials" ), equalTo( (Value) new StringValue( "bar" ) ) );
        assertThat( map.get( "realm" ), equalTo( (Value) new StringValue( "baz" ) ) );
    }

    @Test
    public void customAuthWithoutParameters()
    {
        InternalAuthToken basic = (InternalAuthToken) custom( "foo", "bar", "baz", "my_scheme" );

        Map<String,Value> map = basic.toMap();

        assertThat( map.size(), equalTo( 4 ) );
        assertThat( map.get( "scheme" ), equalTo( (Value) new StringValue( "my_scheme" ) ) );
        assertThat( map.get( "principal" ), equalTo( (Value) new StringValue( "foo" ) ) );
        assertThat( map.get( "credentials" ), equalTo( (Value) new StringValue( "bar" ) ) );
        assertThat( map.get( "realm" ), equalTo( (Value) new StringValue( "baz" ) ) );
    }

    @Test
    public void customAuthParameters()
    {
        HashMap<String,Object> parameters = new HashMap<>();
        parameters.put( "list", asList( 1, 2, 3 ) );
        InternalAuthToken basic = (InternalAuthToken) custom( "foo", "bar", "baz", "my_scheme", parameters );


        Map<String,Value> expectedParameters = new HashMap<>();
        expectedParameters.put( "list", new ListValue( values( 1, 2, 3 ) ) );
        Map<String,Value> map = basic.toMap();

        assertThat( map.size(), equalTo( 5 ) );
        assertThat( map.get( "scheme" ), equalTo( (Value) new StringValue( "my_scheme" ) ) );
        assertThat( map.get( "principal" ), equalTo( (Value) new StringValue( "foo" ) ) );
        assertThat( map.get( "credentials" ), equalTo( (Value) new StringValue( "bar" ) ) );
        assertThat( map.get( "realm" ), equalTo( (Value) new StringValue( "baz" ) ) );
        assertThat( map.get( "parameters" ), equalTo( (Value) new MapValue( expectedParameters ) ) );
    }
}
