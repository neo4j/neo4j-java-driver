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
package org.neo4j.driver.it.encryption.aws_kms;

import java.security.NoSuchAlgorithmException;
import org.neo4j.driver.it.encryption.common.AbstractEnvelopeEncryptionIT;
import org.neo4j.driver.property_encryption.KeyEncapsulationService;
import org.neo4j.driver.property_encryption.aws_kms.AWSKeyEncapsulationService;
import org.neo4j.driver.property_encryption.aws_kms.AwsKeyEncapsulationOptions;

class EnvelopeEncryptionIT extends AbstractEnvelopeEncryptionIT {

    @Override
    protected KeyEncapsulationService keyEncapsulationService() throws NoSuchAlgorithmException {
        return new AWSKeyEncapsulationService(AwsKeyEncapsulationOptions.of("34bec748-e9b0-4be7-99c1-9731ff08b73b"));
    }
}
