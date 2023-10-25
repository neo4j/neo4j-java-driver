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
package org.neo4j.driver.internal;

import java.io.Serial;
import java.io.Serializable;
import org.neo4j.driver.Config;

public record SecuritySettings(boolean encrypted, Config.TrustStrategy trustStrategy) implements Serializable {
    @Serial
    private static final long serialVersionUID = 4494615367164106576L;

    private static final boolean DEFAULT_ENCRYPTED = false;
    private static final Config.TrustStrategy DEFAULT_TRUST_STRATEGY = Config.TrustStrategy.trustSystemCertificates();
    public static final SecuritySettings DEFAULT = new SecuritySettings(DEFAULT_ENCRYPTED, DEFAULT_TRUST_STRATEGY);

    public SecuritySettings(boolean encrypted, Config.TrustStrategy trustStrategy) {
        this.encrypted = encrypted;
        this.trustStrategy = trustStrategy == null ? DEFAULT_TRUST_STRATEGY : trustStrategy;
    }

    public static class SecuritySettingsBuilder {
        private boolean isCustomized = false;
        private boolean encrypted;
        private Config.TrustStrategy trustStrategy;

        public SecuritySettingsBuilder withEncryption() {
            encrypted = true;
            isCustomized = true;
            return this;
        }

        public SecuritySettingsBuilder withoutEncryption() {
            encrypted = false;
            isCustomized = true;
            return this;
        }

        public SecuritySettingsBuilder withTrustStrategy(Config.TrustStrategy strategy) {
            trustStrategy = strategy;
            isCustomized = true;
            return this;
        }

        public SecuritySettings build() {
            return isCustomized ? new SecuritySettings(encrypted, trustStrategy) : SecuritySettings.DEFAULT;
        }
    }
}
