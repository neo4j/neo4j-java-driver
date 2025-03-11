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

import static java.lang.String.format;

import java.util.Optional;
import org.neo4j.bolt.connection.BoltAgent;
import org.neo4j.driver.Session;

public class DriverInfoUtil {
    public static BoltAgent boltAgent() {
        var productInfo = format("neo4j-java/%s", driverVersion());

        var platformBuilder = new StringBuilder();
        getProperty("os.name").ifPresent(value -> append(value, platformBuilder));
        getProperty("os.version").ifPresent(value -> append(value, platformBuilder));
        getProperty("os.arch").ifPresent(value -> append(value, platformBuilder));

        var language = getProperty("java.version").map(version -> "Java/" + version);

        var languageDetails = language.map(ignored -> {
            var languageDetailsBuilder = new StringBuilder();
            getProperty("java.vm.vendor").ifPresent(value -> append(value, languageDetailsBuilder));
            getProperty("java.vm.name").ifPresent(value -> append(value, languageDetailsBuilder));
            getProperty("java.vm.version").ifPresent(value -> append(value, languageDetailsBuilder));
            return languageDetailsBuilder.isEmpty() ? null : languageDetailsBuilder;
        });

        return new BoltAgent(
                productInfo,
                platformBuilder.isEmpty() ? null : platformBuilder.toString(),
                language.orElse(null),
                languageDetails.map(StringBuilder::toString).orElse(null));
    }

    /**
     * Extracts the driver version from the driver jar MANIFEST.MF file.
     */
    public static String driverVersion() {
        // "Session" is arbitrary - the only thing that matters is that the class we use here is in the
        // 'org.neo4j.driver' package, because that is where the jar manifest specifies the version.
        // This is done as part of the build, adding a MANIFEST.MF file to the generated jarfile.
        var pkg = Session.class.getPackage();
        if (pkg != null && pkg.getImplementationVersion() != null) {
            return pkg.getImplementationVersion();
        }

        // If there is no version, we're not running from a jar file, but from raw compiled class files.
        // This should only happen during development, so call the version 'dev'.
        return "dev";
    }

    private static Optional<String> getProperty(String key) {
        try {
            var value = System.getProperty(key);
            if (value != null) {
                value = value.trim();
            }
            return value != null && !value.isEmpty() ? Optional.of(value) : Optional.empty();
        } catch (SecurityException exception) {
            return Optional.empty();
        }
    }

    private static void append(String value, StringBuilder builder) {
        if (value != null && !value.isEmpty()) {
            var separator = builder.isEmpty() ? "" : "; ";
            builder.append(separator).append(value);
        }
    }
}
