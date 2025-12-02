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
package org.neo4j.driver.it.encryption.azure_keyvault;

import java.security.NoSuchAlgorithmException;
import org.junit.jupiter.api.Disabled;
import org.neo4j.driver.it.encryption.common.AbstractReactiveStreamsEnvelopeEncryptionIT;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.azure_keyvault.AzureEncapsulationOptions;
import org.neo4j.driver.property_encryption.azure_keyvault.AzureKeyEncapsulationService;

@Disabled
final class ReactiveStreamsEnvelopeEncryptionIT extends AbstractReactiveStreamsEnvelopeEncryptionIT {

    @Override
    protected KeyEncapsulationService keyEncapsulationService() throws NoSuchAlgorithmException {
        return new AzureKeyEncapsulationService(AzureEncapsulationOptions.of(
                "https://drivers-vault.vault.azure.net/keys/drivers-key/e787f754d7b64583b2749a55e9186096"));
    }
}
