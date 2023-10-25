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
package org.neo4j.driver.internal;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.internal.messaging.v44.BoltProtocolV44;
import org.neo4j.driver.internal.spi.Connection;

public class ImpersonationUtil {
    public static final String IMPERSONATION_UNSUPPORTED_ERROR_MESSAGE =
            "Detected connection that does not support impersonation, please make sure to have all servers running 4.4 version or above and communicating"
                    + " over Bolt version 4.4 or above when using impersonation feature";

    public static Connection ensureImpersonationSupport(Connection connection, String impersonatedUser) {
        if (impersonatedUser != null && !supportsImpersonation(connection)) {
            throw new ClientException(IMPERSONATION_UNSUPPORTED_ERROR_MESSAGE);
        }
        return connection;
    }

    private static boolean supportsImpersonation(Connection connection) {
        return connection.protocol().version().compareTo(BoltProtocolV44.VERSION) >= 0;
    }
}
