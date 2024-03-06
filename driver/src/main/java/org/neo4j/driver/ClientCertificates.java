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

import java.io.File;
import java.util.Objects;
import org.neo4j.driver.internal.InternalClientCertificate;
import org.neo4j.driver.util.Preview;

/**
 * Creates new instances of {@link ClientCertificate}.
 * @since 5.19
 */
@Preview(name = "mTLS")
public final class ClientCertificates {
    private ClientCertificates() {}

    /**
     * Creates a new instance of {@link ClientCertificate} with certificate {@link File} and private key {@link File}.
     * @param certificate the certificate file, must not be {@literal null}
     * @param privateKey the key file, must not be {@literal null}
     * @return the client certificate
     */
    public static ClientCertificate of(File certificate, File privateKey) {
        return of(certificate, privateKey, null);
    }

    /**
     * Creates a new instance of {@link ClientCertificate} with certificate {@link File}, private key {@link File} and key password.
     * @param certificate the certificate file, must not be {@literal null}
     * @param privateKey the key file, must not be {@literal null}
     * @param password the key password
     * @return the client certificate
     */
    public static ClientCertificate of(File certificate, File privateKey, String password) {
        Objects.requireNonNull(certificate);
        Objects.requireNonNull(privateKey);
        return new InternalClientCertificate(certificate, privateKey, password);
    }
}
