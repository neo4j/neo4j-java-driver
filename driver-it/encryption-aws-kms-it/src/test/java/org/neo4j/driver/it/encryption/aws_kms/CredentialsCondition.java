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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.services.kms.KmsAsyncClient;

class CredentialsCondition implements ExecutionCondition {
    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        try (var client = KmsAsyncClient.create()) {
            client.listKeys().join();
        } catch (Exception e) {
            return ConditionEvaluationResult.disabled("Default Credentials unavailable");
        }
        if (OptionsLoader.fromEnv().isEmpty()) {
            return ConditionEvaluationResult.disabled("Default Options environment variable is not set");
        }
        return ConditionEvaluationResult.enabled("Configuration available");
    }
}
