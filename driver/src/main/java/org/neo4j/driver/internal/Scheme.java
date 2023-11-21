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

import java.util.List;

public class Scheme {
    public static final String BOLT_URI_SCHEME = "bolt";
    public static final String BOLT_HIGH_TRUST_URI_SCHEME = "bolt+s";
    public static final String BOLT_LOW_TRUST_URI_SCHEME = "bolt+ssc";
    public static final String NEO4J_URI_SCHEME = "neo4j";
    public static final String NEO4J_HIGH_TRUST_URI_SCHEME = "neo4j+s";
    public static final String NEO4J_LOW_TRUST_URI_SCHEME = "neo4j+ssc";

    public static void validateScheme(String scheme) {
        if (scheme == null) {
            throw new IllegalArgumentException("Scheme must not be null");
        }
        switch (scheme) {
            case BOLT_URI_SCHEME,
                    BOLT_LOW_TRUST_URI_SCHEME,
                    BOLT_HIGH_TRUST_URI_SCHEME,
                    NEO4J_URI_SCHEME,
                    NEO4J_LOW_TRUST_URI_SCHEME,
                    NEO4J_HIGH_TRUST_URI_SCHEME -> {}
            default -> throw new IllegalArgumentException("Invalid address format " + scheme);
        }
    }

    public static boolean isHighTrustScheme(String scheme) {
        return scheme.equals(BOLT_HIGH_TRUST_URI_SCHEME) || scheme.equals(NEO4J_HIGH_TRUST_URI_SCHEME);
    }

    public static boolean isLowTrustScheme(String scheme) {
        return scheme.equals(BOLT_LOW_TRUST_URI_SCHEME) || scheme.equals(NEO4J_LOW_TRUST_URI_SCHEME);
    }

    public static boolean isSecurityScheme(String scheme) {
        return List.of(
                        BOLT_LOW_TRUST_URI_SCHEME,
                        NEO4J_LOW_TRUST_URI_SCHEME,
                        BOLT_HIGH_TRUST_URI_SCHEME,
                        NEO4J_HIGH_TRUST_URI_SCHEME)
                .contains(scheme);
    }

    public static boolean isRoutingScheme(String scheme) {
        return List.of(NEO4J_LOW_TRUST_URI_SCHEME, NEO4J_HIGH_TRUST_URI_SCHEME, NEO4J_URI_SCHEME)
                .contains(scheme);
    }
}
