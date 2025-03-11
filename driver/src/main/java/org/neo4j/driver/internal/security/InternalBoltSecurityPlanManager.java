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
package org.neo4j.driver.internal.security;

import java.util.concurrent.CompletionStage;
import org.neo4j.bolt.connection.SecurityPlan;
import org.neo4j.bolt.connection.SecurityPlans;

public class InternalBoltSecurityPlanManager implements BoltSecurityPlanManager {
    private final org.neo4j.driver.internal.security.SecurityPlan securityPlan;

    InternalBoltSecurityPlanManager(org.neo4j.driver.internal.security.SecurityPlan securityPlan) {
        this.securityPlan = securityPlan;
    }

    @Override
    public CompletionStage<SecurityPlan> plan() {
        return securityPlan
                .sslContext()
                .thenApply(sslContext -> securityPlan.requiresEncryption()
                        ? org.neo4j.bolt.connection.SecurityPlans.encrypted(
                                securityPlan.requiresClientAuth(),
                                sslContext,
                                securityPlan.requiresHostnameVerification())
                        : SecurityPlans.unencrypted());
    }

    @Override
    public boolean requiresEncryption() {
        return securityPlan.requiresEncryption();
    }
}
