/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.v1;

import java.net.URI;
import java.util.List;

import org.neo4j.driver.internal.net.BoltServerAddress;

/**
 * A Neo4j database driver, through which you can create {@link Session sessions} to run statements against the database.
 * <p>
 * A driver maintains a connection pool for each Neo4j instance. For resource efficiency reasons you are encouraged
 * to use the same driver instance across your application. You can control the connection pooling behavior when you
 * create the driver using the {@link Config} you pass into {@link GraphDatabase#driver(URI, Config)}.
 *
 * @since 1.0
 */
public interface Driver extends AutoCloseable
{
    /**
     * Return a collection of the server addresses known by this driver.
     *
     * @return list of server addresses
     */
    List<BoltServerAddress> servers();

    /**
     * Return a flag to indicate whether or not encryption is used for this driver.
     *
     * @return true if the driver requires encryption, false otherwise
     */
    boolean isEncrypted();

    /**
     * Establish a session
     *
     * @return a session that could be used to run {@link Session#run(String) a statement} or
     * {@link Session#beginTransaction() a transaction }.
     */
    Session session();

    /**
     * Close all the resources assigned to this driver
     */
    void close();
}
