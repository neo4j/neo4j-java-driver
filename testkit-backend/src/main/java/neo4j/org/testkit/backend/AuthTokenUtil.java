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
package neo4j.org.testkit.backend;

import java.util.Optional;
import neo4j.org.testkit.backend.messages.requests.AuthorizationToken;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.security.InternalAuthToken;

public class AuthTokenUtil {
    public static AuthToken parseAuthToken(AuthorizationToken authTokenO) {
        return switch (authTokenO.getTokens().getScheme()) {
            case "basic" -> AuthTokens.basic(
                    authTokenO.getTokens().getPrincipal(),
                    authTokenO.getTokens().getCredentials(),
                    authTokenO.getTokens().getRealm());
            case "bearer" -> AuthTokens.bearer(authTokenO.getTokens().getCredentials());
            case "kerberos" -> AuthTokens.kerberos(authTokenO.getTokens().getCredentials());
            default -> AuthTokens.custom(
                    authTokenO.getTokens().getPrincipal(),
                    authTokenO.getTokens().getCredentials(),
                    authTokenO.getTokens().getRealm(),
                    authTokenO.getTokens().getScheme(),
                    authTokenO.getTokens().getParameters());
        };
    }

    public static AuthorizationToken parseAuthToken(AuthToken authToken) {
        var authorizationToken = new AuthorizationToken();
        var tokens = new AuthorizationToken.Tokens();
        authorizationToken.setTokens(tokens);
        var t = ((InternalAuthToken) authToken).toMap();
        tokens.setScheme(Optional.ofNullable(t.get(InternalAuthToken.SCHEME_KEY))
                .map(Value::asString)
                .orElse(null));
        tokens.setPrincipal(Optional.ofNullable(t.get(InternalAuthToken.PRINCIPAL_KEY))
                .map(Value::asString)
                .orElse(null));
        tokens.setCredentials(Optional.ofNullable(t.get(InternalAuthToken.CREDENTIALS_KEY))
                .map(Value::asString)
                .orElse(null));
        tokens.setRealm(Optional.ofNullable(t.get(InternalAuthToken.REALM_KEY))
                .map(Value::asString)
                .orElse(null));
        tokens.setParameters(Optional.ofNullable(t.get(InternalAuthToken.PARAMETERS_KEY))
                .map(Value::asMap)
                .orElse(null));
        return authorizationToken;
    }
}
