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
package org.neo4j.driver.internal.adaptedbolt;

import java.util.Map;
import java.util.concurrent.CompletionException;
import javax.net.ssl.SSLHandshakeException;
import org.neo4j.bolt.connection.exception.BoltClientException;
import org.neo4j.bolt.connection.exception.BoltConnectionAcquisitionException;
import org.neo4j.bolt.connection.exception.BoltConnectionReadTimeoutException;
import org.neo4j.bolt.connection.exception.BoltDiscoveryException;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltGqlErrorException;
import org.neo4j.bolt.connection.exception.BoltProtocolException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.bolt.connection.exception.BoltTransientException;
import org.neo4j.bolt.connection.exception.BoltUnsupportedFeatureException;
import org.neo4j.bolt.connection.exception.BoltUntrustedServerException;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ConnectionReadTimeoutException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ProtocolException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.exceptions.UnsupportedFeatureException;
import org.neo4j.driver.exceptions.UntrustedServerException;
import org.neo4j.driver.internal.GqlStatusError;
import org.neo4j.driver.internal.util.Futures;
import org.neo4j.driver.internal.value.BoltValueFactory;

public class ErrorMapper {
    private static final ErrorMapper INSTANCE = new ErrorMapper();
    private static final BoltValueFactory BOLT_VALUE_FACTORY = BoltValueFactory.getInstance();

    public static ErrorMapper getInstance() {
        return INSTANCE;
    }

    protected ErrorMapper() {}

    <T> T mapAndThrow(Throwable throwable) {
        throwable = map(throwable);
        if (throwable instanceof RuntimeException runtimeException) {
            throw runtimeException;
        } else {
            throw new CompletionException(throwable);
        }
    }

    Throwable map(Throwable throwable) {
        throwable = Futures.completionExceptionCause(throwable);
        var result = throwable;
        try {
            if (throwable instanceof BoltFailureException boltFailureException) {
                result = mapBoltFailureException(boltFailureException);
            } else if (throwable instanceof BoltGqlErrorException boltGqlErrorException) {
                result = mapGqlCause(boltGqlErrorException);
            } else if (throwable instanceof BoltConnectionReadTimeoutException) {
                result = ConnectionReadTimeoutException.INSTANCE;
            } else if (throwable instanceof BoltServiceUnavailableException boltServiceUnavailableException) {
                result = mapServiceUnavailable(boltServiceUnavailableException);
            } else if (throwable instanceof BoltProtocolException) {
                result = new ProtocolException(throwable.getMessage(), throwable);
            } else if (throwable instanceof BoltUnsupportedFeatureException) {
                result = new UnsupportedFeatureException(throwable.getMessage(), throwable);
            } else if (throwable instanceof BoltUntrustedServerException) {
                result = new UntrustedServerException(throwable.getMessage());
            } else if (throwable instanceof SSLHandshakeException) {
                result = new SecurityException("Failed to establish secured connection with the server", throwable);
            } else if (throwable instanceof BoltClientException) {
                result = new ClientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(throwable.getMessage()),
                        "N/A",
                        throwable.getMessage(),
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        throwable);
            } else if (throwable instanceof BoltTransientException) {
                result = new TransientException(
                        GqlStatusError.UNKNOWN.getStatus(),
                        GqlStatusError.UNKNOWN.getStatusDescription(throwable.getMessage()),
                        "N/A",
                        throwable.getMessage(),
                        GqlStatusError.DIAGNOSTIC_RECORD,
                        throwable);
            } else if (throwable instanceof BoltConnectionAcquisitionException) {
                result = new SessionExpiredException(throwable.getMessage(), throwable);
            }
        } catch (Throwable ignored) {
        }
        return result;
    }

    protected Throwable mapBoltFailureException(BoltFailureException boltFailureException) {
        var code = boltFailureException.code();
        var nested = boltFailureException.gqlCause().map(this::mapGqlCause).orElse(null);
        return switch (extractErrorClass(code)) {
            case "ClientError" -> {
                if ("Security".equals(extractErrorSubClass(code))) {
                    if (code.equalsIgnoreCase("Neo.ClientError.Security.Unauthorized")) {
                        yield mapToNeo4jException(AuthenticationException::new, boltFailureException, nested);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Security.AuthorizationExpired")) {
                        yield mapToNeo4jException(AuthorizationExpiredException::new, boltFailureException, nested);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Security.TokenExpired")) {
                        yield mapToNeo4jException(TokenExpiredException::new, boltFailureException, nested);
                    } else {
                        yield mapToNeo4jException(SecurityException::new, boltFailureException, nested);
                    }
                } else {
                    if (code.equalsIgnoreCase("Neo.ClientError.Database.DatabaseNotFound")) {
                        yield mapToNeo4jException(FatalDiscoveryException::new, boltFailureException, nested);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Transaction.Terminated")) {
                        yield mapToNeo4jException(TransactionTerminatedException::new, boltFailureException, nested);
                    } else {
                        yield mapToNeo4jException(ClientException::new, boltFailureException, nested);
                    }
                }
            }
            case "TransientError" -> {
                // Since 5.0 these 2 errors have been moved to ClientError class.
                // This mapping is required if driver is connection to earlier server versions.
                if ("Neo.TransientError.Transaction.Terminated".equals(code)) {
                    yield new TransactionTerminatedException(
                            boltFailureException.gqlStatus(),
                            boltFailureException.statusDescription(),
                            "Neo.ClientError.Transaction.Terminated",
                            boltFailureException.getMessage(),
                            BOLT_VALUE_FACTORY.toDriverMap(boltFailureException.diagnosticRecord()),
                            nested);
                } else if ("Neo.TransientError.Transaction.LockClientStopped".equals(code)) {
                    yield new ClientException(
                            boltFailureException.gqlStatus(),
                            boltFailureException.statusDescription(),
                            "Neo.ClientError.Transaction.LockClientStopped",
                            boltFailureException.getMessage(),
                            BOLT_VALUE_FACTORY.toDriverMap(boltFailureException.diagnosticRecord()),
                            nested);
                } else {
                    yield mapToNeo4jException(TransientException::new, boltFailureException, nested);
                }
            }
            default -> mapToNeo4jException(DatabaseException::new, boltFailureException, nested);
        };
    }

    protected Throwable mapGqlCause(BoltGqlErrorException boltGqlErrorException) {
        return new Neo4jException(
                boltGqlErrorException.gqlStatus(),
                boltGqlErrorException.statusDescription(),
                "N/A",
                boltGqlErrorException.getMessage(),
                BOLT_VALUE_FACTORY.toDriverMap(boltGqlErrorException.diagnosticRecord()),
                boltGqlErrorException.gqlCause().map(this::mapGqlCause).orElse(null));
    }

    protected Throwable mapServiceUnavailable(BoltServiceUnavailableException boltServiceUnavailableException) {
        Throwable result = null;

        // A BoltServiceUnavailableException exception having suppressed BoltDiscoveryException must be mapped to
        // ServiceUnavailableException with suppressed DiscoveryException.
        for (var suppressed : boltServiceUnavailableException.getSuppressed()) {
            if (suppressed instanceof BoltDiscoveryException boltDiscoveryException) {
                if (result == null) {
                    result = new ServiceUnavailableException(boltServiceUnavailableException.getMessage());
                }
                // The DiscoveryException must have the original exception sequence.
                result.addSuppressed(
                        new DiscoveryException(boltDiscoveryException.getMessage(), boltDiscoveryException));
            }
        }

        if (result == null) {
            result = new ServiceUnavailableException(
                    boltServiceUnavailableException.getMessage(), boltServiceUnavailableException);
        }

        return result;
    }

    private static String extractErrorClass(String code) {
        var parts = code.split("\\.");
        if (parts.length < 2) {
            return "";
        }
        return parts[1];
    }

    private static String extractErrorSubClass(String code) {
        var parts = code.split("\\.");
        if (parts.length < 3) {
            return "";
        }
        return parts[2];
    }

    private <T extends Neo4jException> T mapToNeo4jException(
            Neo4jExceptionBuilder<T> builder, BoltFailureException boltFailureException, Throwable cause) {
        return builder.build(
                boltFailureException.gqlStatus(),
                boltFailureException.statusDescription(),
                boltFailureException.code(),
                boltFailureException.getMessage(),
                BOLT_VALUE_FACTORY.toDriverMap(boltFailureException.diagnosticRecord()),
                cause);
    }

    @FunctionalInterface
    private interface Neo4jExceptionBuilder<T> {
        T build(
                String gqlStatus,
                String statusDescription,
                String code,
                String message,
                Map<String, Value> diagnosticRecord,
                Throwable cause);
    }
}
