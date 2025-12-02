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

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

class CredentialsCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        var optionsOpt = OptionsLoader.fromEnv();
        if (optionsOpt.isEmpty()) {
            return ConditionEvaluationResult.disabled("Default Options environment variable is not set");
        }
        try {
            new CryptographyClientBuilder()
                    .credential(new DefaultAzureCredentialBuilder().build())
                    .keyIdentifier(optionsOpt.get().keyId())
                    .buildAsyncClient();
        } catch (Exception e) {
            return ConditionEvaluationResult.disabled("Default Credentials unavailable");
        }
        return ConditionEvaluationResult.enabled("Configuration available");
    }
}
