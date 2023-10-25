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
package org.neo4j.driver.internal.util;

import static java.lang.Integer.compare;
import static java.util.Objects.requireNonNull;

public enum Neo4jFeature {
    SPATIAL_TYPES(new Version(3, 4, 0)),
    TEMPORAL_TYPES(new Version(3, 4, 0)),
    SERVER_SIDE_ROUTING_ENABLED_BY_DEFAULT(new Version(5, 0, 0)),
    BOLT_V3(new Version(3, 5, 0)),
    BOLT_V4(new Version(4, 0, 0)),
    BOLT_V51(new Version(5, 5, 0));

    private final Version availableFromVersion;

    Neo4jFeature(Version availableFromVersion) {
        this.availableFromVersion = requireNonNull(availableFromVersion);
    }

    public boolean availableIn(Version version) {
        return version.greaterThanOrEqual(availableFromVersion);
    }

    public static class Version {

        private final int major;
        private final int minor;
        private final int patch;

        public Version(int major, int minor, int patch) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
        }

        public boolean greaterThanOrEqual(Version other) {
            return compareTo(other) >= 0;
        }

        private int compareTo(Version o) {
            var c = compare(major, o.major);
            if (c == 0) {
                c = compare(minor, o.minor);
                if (c == 0) {
                    c = compare(patch, o.patch);
                }
            }
            return c;
        }
    }
}
