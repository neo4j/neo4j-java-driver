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
package org.neo4j.driver.internal.async;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.message.Message;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.SecurityRetryableException;
import org.neo4j.driver.internal.adaptedbolt.DriverBoltConnection;
import org.neo4j.driver.internal.adaptedbolt.DriverResponseHandler;
import org.neo4j.driver.internal.security.InternalAuthToken;
import org.neo4j.driver.internal.value.BoltValueFactory;

final class BoltConnectionWithAuthTokenManager extends DelegatingBoltConnection {
    private final AuthTokenManager authTokenManager;

    public BoltConnectionWithAuthTokenManager(DriverBoltConnection delegate, AuthTokenManager authTokenManager) {
        super(delegate);
        this.authTokenManager = Objects.requireNonNull(authTokenManager);
    }

    @Override
    public CompletionStage<Void> writeAndFlush(DriverResponseHandler handler, List<Message> messages) {
        return delegate.writeAndFlush(new ErrorMappingResponseHandler(handler, this::mapSecurityError), messages);
    }

    private Throwable mapSecurityError(Throwable throwable) {
        if (throwable instanceof SecurityException securityException) {
            var authInfo = delegate.authData().toCompletableFuture().getNow(null);
            if (authInfo != null
                    && authTokenManager.handleSecurityException(
                            new InternalAuthToken(BoltValueFactory.getInstance()
                                    .toDriverMap(authInfo.authToken().asMap())),
                            securityException)) {
                throwable = new SecurityRetryableException(securityException);
            }
        }
        return throwable;
    }
}
