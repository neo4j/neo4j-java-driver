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

import static java.lang.String.format;

import java.util.Objects;
import org.neo4j.bolt.connection.AccessMode;
import org.neo4j.bolt.connection.BoltServerAddress;
import org.neo4j.bolt.connection.exception.BoltFailureException;
import org.neo4j.bolt.connection.exception.BoltServiceUnavailableException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.GqlStatusError;

class RoutedErrorMapper extends ErrorMapper {
    private final BoltServerAddress address;
    private final AccessMode accessMode;

    RoutedErrorMapper(BoltServerAddress address, AccessMode accessMode) {
        this.address = Objects.requireNonNull(address);
        this.accessMode = Objects.requireNonNull(accessMode);
    }

    @Override
    protected Throwable mapBoltFailureException(BoltFailureException boltFailureException) {
        Throwable result;
        if ("Neo.ClientError.Cluster.NotALeader".equals(boltFailureException.code())
                || "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase".equals(boltFailureException.code())) {
            result = switch (accessMode) {
                case READ -> {
                    var message = "Write queries cannot be performed in READ access mode.";
                    yield new ClientException(
                            GqlStatusError.UNKNOWN.getStatus(),
                            GqlStatusError.UNKNOWN.getStatusDescription(message),
                            "N/A",
                            message,
                            GqlStatusError.DIAGNOSTIC_RECORD,
                            boltFailureException
                                    .gqlCause()
                                    .map(this::mapGqlCause)
                                    .orElse(null));
                }
                case WRITE -> new SessionExpiredException(
                        format("Server at %s no longer accepts writes", address), boltFailureException);};
        } else {
            result = super.mapBoltFailureException(boltFailureException);
        }
        return result;
    }

    @Override
    protected Throwable mapServiceUnavailable(BoltServiceUnavailableException boltServiceUnavailableException) {
        return new SessionExpiredException(
                format("Server at %s is no longer available", address), boltServiceUnavailableException);
    }
}
