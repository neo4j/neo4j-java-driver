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

import org.neo4j.driver.util.Preview;

/**
 * A {@link ClientCertificateManager} that supports rotating (updating) its {@link ClientCertificate}.
 * @since 5.19
 */
@Preview(name = "mTLS")
public sealed interface RotatingClientCertificateManager extends ClientCertificateManager
        permits org.neo4j.driver.internal.InternalRotatingClientCertificateManager {
    /**
     * Updates the current {@link ClientCertificate}.
     * <p>
     * Certificates with {@code hasUpdate = false} will be ignored.
     * @param clientCertificate the new certificate, must not be {@literal null}
     */
    void update(ClientCertificate clientCertificate);
}
