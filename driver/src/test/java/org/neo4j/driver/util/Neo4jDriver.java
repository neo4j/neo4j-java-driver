/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

/**
 * Create a static driver with default settings for tests.
 * This driver and its connection pool are reused by all tests using {@line TestSession} and {@link TestNeo4j}.
 * Any test that does not want to create its own driver could reuse sessions from this driver.
 * Finally this driver will not be destroyed but live in jvm all the time.
 */
public class Neo4jDriver
{
    private static Driver driver = GraphDatabase.driver( Neo4jRunner.DEFAULT_URL );

    public static Session session()
    {
        return driver.session();
    }
}
