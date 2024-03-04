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
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.util.Preview;

/**
 * A manager of {@link ClientCertificate} instances used by the driver for mTLS.
 * <p>
 * The driver uses the {@link ClientCertificate} supplied by the manager for setting up new connections. Therefore,
 * a change of the certificate affects new connections only.
 * <p>
 * For efficiency reasons, the driver will only reload the certificate and the key from the files when the
 * {@code hasUpdate} flag is set to {@literal true}. See {@link ClientCertificates#of(File, File, boolean)} and
 * {@link ClientCertificates#of(File, File, String, boolean)}.
 * <p>
 * The manager must never return {@literal null} or {@link CompletionStage} completing with {@literal null}.
 * <p>
 * All implementations of this interface must be thread-safe and non-blocking for caller threads. For instance, IO
 * operations must not done on the calling thread.
 * @since 5.19
 */
@Preview(name = "mTLS")
public interface ClientCertificateManager {
    /**
     * Returns a {@link CompletionStage} of the {@link ClientCertificate}.
     * @return the certificate stage, must not be {@literal null} or complete with {@literal null}
     */
    CompletionStage<ClientCertificate> getClientCertificate();
}
