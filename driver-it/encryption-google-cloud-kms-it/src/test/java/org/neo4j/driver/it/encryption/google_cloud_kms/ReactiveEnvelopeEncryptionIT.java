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
package org.neo4j.driver.it.encryption.google_cloud_kms;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.driver.encryption.async.AsyncKeyEncapsulationService;
import org.neo4j.driver.encryption.google_cloud_kms.CloudKeyEncapsulationServices;
import org.neo4j.driver.it.encryption.common.AbstractReactiveEnvelopeEncryptionIT;

@ExtendWith(CredentialsCondition.class)
public final class ReactiveEnvelopeEncryptionIT extends AbstractReactiveEnvelopeEncryptionIT {
    @Override
    protected AsyncKeyEncapsulationService keyEncapsulationService() throws IOException, NoSuchAlgorithmException {
        return CloudKeyEncapsulationServices.create(OptionsLoader.fromEnv().orElseThrow());
    }
}
