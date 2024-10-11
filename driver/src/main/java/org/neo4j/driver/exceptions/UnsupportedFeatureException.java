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
import org.neo4j.driver.internal.GqlStatusError;

/**
 * A feature is not supported in a given setup.
 * @since 5.7
 */
public class UnsupportedFeatureException extends ClientException {
    @Serial
    private static final long serialVersionUID = 1161333003602398544L;

    /**
     * Constructs a new instance.
     * @param message the message
     */
    public UnsupportedFeatureException(String message) {
        this(message, null);
    }

    /**
     * Constructs a new instance.
     * @param message the message
     * @param cause the cause
     */
    public UnsupportedFeatureException(String message, Throwable cause) {
        super(
                GqlStatusError.UNKNOWN.getStatus(),
                GqlStatusError.UNKNOWN.getStatusDescription(message),
                "N/A",
                message,
                GqlStatusError.DIAGNOSTIC_RECORD,
                cause);
    }
}
