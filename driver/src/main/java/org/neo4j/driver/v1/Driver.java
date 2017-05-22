/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

/**
 * Accessor for a specific Neo4j graph database.
 * <p>
 * Driver implementations are typically thread-safe, act as a template
 * for {@link Session} creation and host a connection pool. All configuration
 * and authentication settings are held immutably by the Driver. Should
 * different settings be required, a new Driver instance should be created.
 * <p>
 * A driver maintains a connection pool for each remote Neo4j server. Therefore
 * the most efficient way to make use of a Driver is to use the same instance
 * across the application.
 * <p>
 * To construct a new Driver, use one of the
 * {@link GraphDatabase#driver(String, AuthToken) GraphDatabase.driver} methods.
 * The <a href="https://tools.ietf.org/html/rfc3986">URI</a> passed to
 * this method determines the type of Driver created.
 * <br>
 * <table border="1" cellpadding="4" style="border-collapse: collapse" summary="Available schemes and drivers">
 *     <thead>
 *         <tr><th>URI Scheme</th><th>Driver</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr>
 *             <td><code>bolt</code></td>
 *             <td>Direct driver: connects directly to the host and port specified in the URI.</td>
 *         </tr>
 *         <tr>
 *             <td><code>bolt+routing</code></td>
 *             <td>Routing driver: can automatically discover members of a Causal Cluster and route {@link Session sessions} based on {@link AccessMode}.</td>
 *         </tr>
 *     </tbody>
 * </table>
 *
 * @since 1.0 (<em>bolt+routing</em> URIs since 1.1)
 */
public interface Driver extends AutoCloseable
{
    /**
     * Return a flag to indicate whether or not encryption is used for this driver.
     *
     * @return true if the driver requires encryption, false otherwise
     */
    boolean isEncrypted();

    /**
     * Create a new general purpose {@link Session}.
     * <p>
     * Alias to {@code session(AccessMode.WRITE, null)}.
     *
     * @return a new {@link Session} object.
     */
    Session session();

    /**
     * Create a new {@link Session} for a specific type of work.
     * <p>
     * Alias to {@code session(mode, null)}.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @return a new {@link Session} object.
     */
    Session session( AccessMode mode );

    /**
     * Create a new {@link AccessMode#WRITE write} {@link Session} with the specified initial bookmark.
     * First transaction in the created session will ensure that server hosting is at least as up-to-date as the
     * transaction referenced by the supplied <em>bookmark</em>.
     * <p>
     * Alias to {@code session(AccessMode.WRITE, bookmark)}.
     *
     * @param bookmark the initial reference to some previous transaction. A {@code null} value is permitted, and
     * indicates that the bookmark does not exist or is unknown.
     * @return a new {@link Session} object.
     */
    Session session( String bookmark );

    /**
     * Create a new {@link Session} for a specific type of work with the specified initial bookmark.
     * First transaction in the created session will ensure that server hosting is at least as up-to-date as the
     * transaction referenced by the supplied <em>bookmark</em>.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @param bookmark the initial reference to some previous transaction. A {@code null} value is permitted, and
     * indicates that the bookmark does not exist or is unknown.
     * @return a new {@link Session} object.
     */
    Session session( AccessMode mode, String bookmark );

    /**
     * Create a new {@link AccessMode#WRITE write} {@link Session} with specified initial bookmarks.
     * First transaction in the created session will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied iterable of bookmarks.
     * <p>
     * Alias to {@code session(AccessMode.WRITE, bookmarks)}.
     *
     * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable
     * are permitted, and indicate that the bookmarks do not exist or are unknown.
     * @return a new {@link Session} object.
     */
    Session session( Iterable<String> bookmarks );

    /**
     * Create a new {@link AccessMode#WRITE write} {@link Session} with specified initial bookmarks.
     * First transaction in the created session will ensure that server hosting is at least as up-to-date as the
     * latest transaction referenced by the supplied iterable of bookmarks.
     * <p>
     * Alias to {@code session(AccessMode.WRITE, bookmarks)}.
     *
     * @param mode the type of access required by units of work in this session,
     * e.g. {@link AccessMode#READ read access} or {@link AccessMode#WRITE write access}.
     * @param bookmarks initial references to some previous transactions. Both {@code null} value and empty iterable
     * are permitted, and indicate that the bookmarks do not exist or are unknown.
     * @return a new {@link Session} object.
     */
    Session session( AccessMode mode, Iterable<String> bookmarks );

    /**
     * Close all the resources assigned to this driver, including any open connections.
     */
    void close();
}
