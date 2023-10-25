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
package org.neo4j.driver.exceptions;

import java.io.Serial;
import java.util.Objects;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.util.Experimental;

/**
 * Indicates that the contained {@link SecurityException} is a {@link RetryableException}, which is determined by the
 * {@link org.neo4j.driver.AuthTokenManager#handleSecurityException(AuthToken, SecurityException)} method.
 * <p>
 * The original {@link java.lang.SecurityException} is always available as a {@link Throwable#getCause()}. The
 * {@link SecurityRetryableException#code()} and {@link SecurityRetryableException#getMessage()} supply the values from
 * the original exception.
 *
 * @since 5.12
 */
public class SecurityRetryableException extends SecurityException implements RetryableException {
    @Serial
    private static final long serialVersionUID = 3914900631374208080L;

    /**
     * The original security exception.
     */
    private final SecurityException exception;

    /**
     * Creates a new instance.
     *
     * @param exception the original security exception, must not be {@code null}
     */
    public SecurityRetryableException(SecurityException exception) {
        super(exception.getMessage(), exception);
        this.exception = Objects.requireNonNull(exception);
    }

    @Override
    public String code() {
        return exception.code();
    }

    @Override
    public String getMessage() {
        return exception.getMessage();
    }

    /**
     * Returns the original security exception.
     *
     * @return the original security exception
     */
    @Experimental
    public SecurityException securityException() {
        return exception;
    }
}
