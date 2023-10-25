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
package org.neo4j.driver.internal.util;

import java.io.Serial;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.driver.exceptions.AuthorizationExpiredException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.FatalDiscoveryException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.ResultConsumedException;
import org.neo4j.driver.exceptions.SecurityException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TokenExpiredException;
import org.neo4j.driver.exceptions.TransactionTerminatedException;
import org.neo4j.driver.exceptions.TransientException;

public final class ErrorUtil {
    private ErrorUtil() {}

    public static ServiceUnavailableException newConnectionTerminatedError(String reason) {
        if (reason == null) {
            return newConnectionTerminatedError();
        }
        return new ServiceUnavailableException("Connection to the database terminated. " + reason);
    }

    public static ServiceUnavailableException newConnectionTerminatedError() {
        return new ServiceUnavailableException("Connection to the database terminated. "
                + "Please ensure that your database is listening on the correct host and port and that you have compatible encryption settings both on Neo4j server and driver. "
                + "Note that the default encryption setting has changed in Neo4j 4.0.");
    }

    public static ResultConsumedException newResultConsumedError() {
        return new ResultConsumedException(
                "Cannot access records on this result any more as the result has already been consumed "
                        + "or the query runner where the result is created has already been closed.");
    }

    public static Neo4jException newNeo4jError(String code, String message) {
        switch (extractErrorClass(code)) {
            case "ClientError" -> {
                if ("Security".equals(extractErrorSubClass(code))) {
                    if (code.equalsIgnoreCase("Neo.ClientError.Security.Unauthorized")) {
                        return new AuthenticationException(code, message);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Security.AuthorizationExpired")) {
                        return new AuthorizationExpiredException(code, message);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Security.TokenExpired")) {
                        return new TokenExpiredException(code, message);
                    } else {
                        return new SecurityException(code, message);
                    }
                } else {
                    if (code.equalsIgnoreCase("Neo.ClientError.Database.DatabaseNotFound")) {
                        return new FatalDiscoveryException(code, message);
                    } else if (code.equalsIgnoreCase("Neo.ClientError.Transaction.Terminated")) {
                        return new TransactionTerminatedException(code, message);
                    } else {
                        return new ClientException(code, message);
                    }
                }
            }
            case "TransientError" -> {
                // Since 5.0 these 2 errors have been moved to ClientError class.
                // This mapping is required if driver is connection to earlier server versions.
                if ("Neo.TransientError.Transaction.Terminated".equals(code)) {
                    return new TransactionTerminatedException("Neo.ClientError.Transaction.Terminated", message);
                } else if ("Neo.TransientError.Transaction.LockClientStopped".equals(code)) {
                    return new ClientException("Neo.ClientError.Transaction.LockClientStopped", message);
                } else {
                    return new TransientException(code, message);
                }
            }
            default -> {
                return new DatabaseException(code, message);
            }
        }
    }

    public static boolean isFatal(Throwable error) {
        if (error instanceof Neo4jException) {
            if (isProtocolViolationError(((Neo4jException) error))) {
                return true;
            }

            return !isClientOrTransientError(((Neo4jException) error));
        }
        return true;
    }

    public static void rethrowAsyncException(ExecutionException e) {
        var error = e.getCause();

        var internalCause = new InternalExceptionCause(error.getStackTrace());
        error.addSuppressed(internalCause);

        var currentStackTrace = Stream.of(Thread.currentThread().getStackTrace())
                .skip(2) // do not include Thread.currentThread() and this method in the stacktrace
                .toArray(StackTraceElement[]::new);
        error.setStackTrace(currentStackTrace);

        RuntimeException exception;
        if (error instanceof RuntimeException) {
            exception = (RuntimeException) error;
        } else {
            exception = new Neo4jException("Driver execution failed", error);
        }
        throw exception;
    }

    private static boolean isProtocolViolationError(Neo4jException error) {
        var errorCode = error.code();
        return errorCode != null && errorCode.startsWith("Neo.ClientError.Request");
    }

    private static boolean isClientOrTransientError(Neo4jException error) {
        var errorCode = error.code();
        return errorCode != null && (errorCode.contains("ClientError") || errorCode.contains("TransientError"));
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

    public static void addSuppressed(Throwable mainError, Throwable error) {
        if (mainError != error) {
            mainError.addSuppressed(error);
        }
    }

    /**
     * Exception which is merely a holder of an async stacktrace, which is not the primary stacktrace users are interested in.
     * Used for blocking API calls that block on async API calls.
     */
    private static class InternalExceptionCause extends RuntimeException {
        @Serial
        private static final long serialVersionUID = -1988733529334222027L;

        InternalExceptionCause(StackTraceElement[] stackTrace) {
            setStackTrace(stackTrace);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            // no need to fill in the stack trace
            // this exception just uses the given stack trace
            return this;
        }
    }
}
