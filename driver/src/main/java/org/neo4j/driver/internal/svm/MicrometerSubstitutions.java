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
package org.neo4j.driver.internal.svm;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.time.Clock;
import org.neo4j.driver.Config;
import org.neo4j.driver.MetricsAdapter;
import org.neo4j.driver.internal.DriverFactory;
import org.neo4j.driver.internal.metrics.DevNullMetricsProvider;
import org.neo4j.driver.internal.metrics.InternalMetricsProvider;
import org.neo4j.driver.internal.metrics.MetricsProvider;
import org.neo4j.driver.internal.metrics.MicrometerMetricsProvider;

@TargetClass(DriverFactory.class)
final class Target_org_neo4j_driver_internal_DriverFactory {

    /**
     * Substitutes metrics adapter in such a way that it falls back to off when Micrometer is not available.
     *
     * @param config Drivers config
     * @param clock Clock to use
     * @return A metrics provider, never null
     */
    @Substitute
    @SuppressWarnings({"ProtectedMemberInFinalClass", "deprecation"})
    protected static MetricsProvider getOrCreateMetricsProvider(Config config, Clock clock) {
        var metricsAdapter = config.metricsAdapter();
        if (metricsAdapter == null) {
            metricsAdapter = config.isMetricsEnabled() ? MetricsAdapter.DEFAULT : MetricsAdapter.DEV_NULL;
        }
        switch (metricsAdapter) {
            case DEV_NULL -> {
                return DevNullMetricsProvider.INSTANCE;
            }
            case DEFAULT -> {
                return new InternalMetricsProvider(clock, config.logging());
            }
            case MICROMETER -> {
                try {
                    @SuppressWarnings("unused")
                    var metricsClass = Class.forName("io.micrometer.core.instrument.Metrics");
                    return MicrometerMetricsProvider.forGlobalRegistry();
                } catch (ClassNotFoundException e) {
                    return DevNullMetricsProvider.INSTANCE;
                }
            }
        }
        throw new IllegalStateException("Unknown or unsupported MetricsAdapter: " + metricsAdapter);
    }
}

class MicrometerSubstitutions {}
