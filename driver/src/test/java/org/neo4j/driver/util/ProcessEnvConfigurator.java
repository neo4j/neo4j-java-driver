/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
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
package org.neo4j.driver.util;

public final class ProcessEnvConfigurator
{
    /**
     * Name of environment variable used by the Neo4j database.
     */
    private static final String JAVA_HOME = "JAVA_HOME";
    /**
     * Name of environment variable to be used for the Neo4j database, defined by the build system.
     */
    private static final String NEO4J_JAVA = "NEO4J_JAVA";
    /**
     * Name of an optional environment variable containing file path to a local Neo4j package.
     * This package is used by boltkit instead of downloading a package with the specified Neo4j version.
     */
    private static final String BOLTKIT_LOCAL_PACKAGE = "NEOCTRL_LOCAL_PACKAGE";

    private ProcessEnvConfigurator()
    {
    }

    public static void configure( ProcessBuilder processBuilder )
    {
        processBuilder.environment().put( JAVA_HOME, determineJavaHome() );

        String localPackage = determineLocalPackage();
        if ( localPackage != null )
        {
            processBuilder.environment().put( BOLTKIT_LOCAL_PACKAGE, localPackage );
        }
    }

    /**
     * This driver is built to work with multiple java versions. Neo4j, however, works with a specific version of
     * Java. This allows specifying which Java version to use for Neo4j separately from which version to use for
     * the driver tests.
     * <p>
     * This method determines which java home to use based on present environment variables.
     *
     * @return path to the java home.
     */
    private static String determineJavaHome()
    {
        return System.getenv().getOrDefault( NEO4J_JAVA, System.getProperties().getProperty( "java.home" ) );
    }

    private static String determineLocalPackage()
    {
        String value = System.getenv().getOrDefault( BOLTKIT_LOCAL_PACKAGE, "" ).trim();
        return value.isEmpty() ? null : value;
    }
}
