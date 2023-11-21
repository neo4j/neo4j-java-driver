/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.driver.AuthTokens.basic;
import static org.neo4j.driver.AuthTokens.custom;
import static org.neo4j.driver.Values.values;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.StringValue;

class AuthTokensTest {
    @Test
    void basicAuthWithoutRealm() {
        var basic = (InternalAuthToken) basic("foo", "bar");

        var map = basic.toMap();

        assertThat(map.size(), equalTo(3));
        assertThat(map.get("scheme"), equalTo(new StringValue("basic")));
        assertThat(map.get("principal"), equalTo(new StringValue("foo")));
        assertThat(map.get("credentials"), equalTo(new StringValue("bar")));
    }

    @Test
    void basicAuthWithRealm() {
        var basic = (InternalAuthToken) basic("foo", "bar", "baz");

        var map = basic.toMap();

        assertThat(map.size(), equalTo(4));
        assertThat(map.get("scheme"), equalTo(new StringValue("basic")));
        assertThat(map.get("principal"), equalTo(new StringValue("foo")));
        assertThat(map.get("credentials"), equalTo(new StringValue("bar")));
        assertThat(map.get("realm"), equalTo(new StringValue("baz")));
    }

    @Test
    void customAuthWithoutParameters() {
        var basic = (InternalAuthToken) custom("foo", "bar", "baz", "my_scheme");

        var map = basic.toMap();

        assertThat(map.size(), equalTo(4));
        assertThat(map.get("scheme"), equalTo(new StringValue("my_scheme")));
        assertThat(map.get("principal"), equalTo(new StringValue("foo")));
        assertThat(map.get("credentials"), equalTo(new StringValue("bar")));
        assertThat(map.get("realm"), equalTo(new StringValue("baz")));
    }

    @Test
    void customAuthParameters() {
        var parameters = new HashMap<String, Object>();
        parameters.put("list", asList(1, 2, 3));
        var basic = (InternalAuthToken) custom("foo", "bar", "baz", "my_scheme", parameters);

        Map<String, Value> expectedParameters = new HashMap<>();
        expectedParameters.put("list", new ListValue(values(1, 2, 3)));
        var map = basic.toMap();

        assertThat(map.size(), equalTo(5));
        assertThat(map.get("scheme"), equalTo(new StringValue("my_scheme")));
        assertThat(map.get("principal"), equalTo(new StringValue("foo")));
        assertThat(map.get("credentials"), equalTo(new StringValue("bar")));
        assertThat(map.get("realm"), equalTo(new StringValue("baz")));
        assertThat(map.get("parameters"), equalTo(new MapValue(expectedParameters)));
    }

    @Test
    void shouldSupportBearerAuth() {
        // GIVEN
        var tokenStr = "token";

        // WHEN
        var token = (InternalAuthToken) AuthTokens.bearer(tokenStr);

        // THEN
        var map = token.toMap();
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("scheme"), equalTo(new StringValue("bearer")));
        assertThat(map.get("credentials"), equalTo(new StringValue(tokenStr)));
    }

    @Test
    void basicKerberosAuthWithRealm() {
        var token = (InternalAuthToken) AuthTokens.kerberos("base64");
        var map = token.toMap();

        assertThat(map.size(), equalTo(3));
        assertThat(map.get("scheme"), equalTo(new StringValue("kerberos")));
        assertThat(map.get("principal"), equalTo(new StringValue("")));
        assertThat(map.get("credentials"), equalTo(new StringValue("base64")));
    }

    @Test
    void shouldNotAllowBasicAuthTokenWithNullUsername() {
        var e = assertThrows(NullPointerException.class, () -> AuthTokens.basic(null, "password"));
        assertEquals("Username can't be null", e.getMessage());
    }

    @Test
    void shouldNotAllowBasicAuthTokenWithNullPassword() {
        var e = assertThrows(NullPointerException.class, () -> AuthTokens.basic("username", null));
        assertEquals("Password can't be null", e.getMessage());
    }

    @Test
    void shouldAllowBasicAuthTokenWithNullRealm() {
        var token = AuthTokens.basic("username", "password", null);
        var map = ((InternalAuthToken) token).toMap();

        assertEquals(3, map.size());
        assertEquals("basic", map.get("scheme").asString());
        assertEquals("username", map.get("principal").asString());
        assertEquals("password", map.get("credentials").asString());
    }

    @Test
    void shouldNotAllowBearerAuthTokenWithNullToken() {
        var e = assertThrows(NullPointerException.class, () -> AuthTokens.bearer(null));
        assertEquals("Token can't be null", e.getMessage());
    }

    @Test
    void shouldNotAllowKerberosAuthTokenWithNullTicket() {
        var e = assertThrows(NullPointerException.class, () -> AuthTokens.kerberos(null));
        assertEquals("Ticket can't be null", e.getMessage());
    }

    @Test
    void shouldNotAllowCustomAuthTokenWithNullPrincipal() {
        var e = assertThrows(
                NullPointerException.class, () -> AuthTokens.custom(null, "credentials", "realm", "scheme"));
        assertEquals("Principal can't be null", e.getMessage());
    }

    @Test
    void shouldNotAllowCustomAuthTokenWithNullCredentials() {
        var e = assertThrows(NullPointerException.class, () -> AuthTokens.custom("principal", null, "realm", "scheme"));
        assertEquals("Credentials can't be null", e.getMessage());
    }

    @Test
    void shouldAllowCustomAuthTokenWithNullRealm() {
        var token = AuthTokens.custom("principal", "credentials", null, "scheme");
        var map = ((InternalAuthToken) token).toMap();

        assertEquals(3, map.size());
        assertEquals("scheme", map.get("scheme").asString());
        assertEquals("principal", map.get("principal").asString());
        assertEquals("credentials", map.get("credentials").asString());
    }

    @Test
    void shouldNotAllowCustomAuthTokenWithNullScheme() {
        var e = assertThrows(
                NullPointerException.class, () -> AuthTokens.custom("principal", "credentials", "realm", null));
        assertEquals("Scheme can't be null", e.getMessage());
    }
}
