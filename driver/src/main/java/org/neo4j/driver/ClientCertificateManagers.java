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

import org.neo4j.driver.internal.InternalRotatingClientCertificateManager;
import org.neo4j.driver.util.Preview;

/**
 * Implementations of {@link ClientCertificateManager}.
 *
 * @since 5.19
 */
@Preview(name = "mTLS")
public final class ClientCertificateManagers {
    private ClientCertificateManagers() {}

    /**
     * Returns a {@link RotatingClientCertificateManager} that supports rotating its {@link ClientCertificate} using the
     * {@link RotatingClientCertificateManager#rotate(ClientCertificate)} method.
     *
     * @param clientCertificate an initial certificate, must not be {@literal null}
     * @return a new manager
     */
    public static RotatingClientCertificateManager rotating(ClientCertificate clientCertificate) {
        return new InternalRotatingClientCertificateManager(clientCertificate);
    }
}
