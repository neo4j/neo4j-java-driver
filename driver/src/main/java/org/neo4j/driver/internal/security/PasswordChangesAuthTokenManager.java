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
package org.neo4j.driver.internal.security;

import static java.util.Objects.requireNonNull;
import static org.neo4j.driver.internal.util.Futures.failedFuture;
import static org.neo4j.driver.internal.util.LockUtil.executeWithLock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokenManager;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.util.Preview;

@Preview(name = "Password rotation and session auth support")
public class PasswordChangesAuthTokenManager implements AuthTokenManager {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Supplier<CompletionStage<AuthToken>> freshTokenSupplier;

    private CompletableFuture<AuthToken> tokenFuture;
    private AuthToken token;

    public PasswordChangesAuthTokenManager(Supplier<CompletionStage<AuthToken>> freshTokenSupplier) {
        this.freshTokenSupplier = freshTokenSupplier;
    }

    @Override
    public CompletionStage<AuthToken> getToken() {
        var validTokenFuture = executeWithLock(lock.readLock(), this::getValidTokenFuture);
        if (validTokenFuture == null) {
            var fetchFromUpstream = new AtomicBoolean();
            validTokenFuture = executeWithLock(lock.writeLock(), () -> {
                if (getValidTokenFuture() == null) {
                    tokenFuture = new CompletableFuture<>();
                    token = null;
                    fetchFromUpstream.set(true);
                }
                return tokenFuture;
            });
            if (fetchFromUpstream.get()) {
                getFromUpstream().whenComplete(this::handleUpstreamResult);
            }
        }
        return validTokenFuture;
    }

    private CompletableFuture<AuthToken> getValidTokenFuture() {
        CompletableFuture<AuthToken> validTokenFuture = null;
        if (tokenFuture != null) {
            validTokenFuture = tokenFuture;
        }
        return validTokenFuture;
    }

    private CompletionStage<AuthToken> getFromUpstream() {
        CompletionStage<AuthToken> upstreamStage;
        try {
            upstreamStage = freshTokenSupplier.get();
            requireNonNull(upstreamStage, "upstream supplied a null value");
        } catch (Throwable t) {
            upstreamStage = failedFuture(t);
        }
        return upstreamStage;
    }

    private void handleUpstreamResult(AuthToken authToken, Throwable throwable) {
        if (throwable != null) {
            var previousTokenFuture = executeWithLock(lock.writeLock(), this::unsetTokenState);
            // notify downstream consumers of the failure
            previousTokenFuture.completeExceptionally(throwable);
        } else {
            var currentTokenFuture = executeWithLock(lock.writeLock(), () -> {
                token = authToken;
                return tokenFuture;
            });
            currentTokenFuture.complete(authToken);
        }
    }

    private CompletableFuture<AuthToken> unsetTokenState() {
        var previousTokenFuture = tokenFuture;
        tokenFuture = null;
        token = null;
        return previousTokenFuture;
    }

    @Override
    public void onSecurityException(AuthToken authToken, SecurityException exception) {
        executeWithLock(lock.writeLock(), () -> {
            if (token != null && token.equals(authToken)) {
                unsetTokenState();
            }
        });
    }
}
