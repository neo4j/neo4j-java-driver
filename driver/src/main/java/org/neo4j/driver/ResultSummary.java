/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

/**
 * Contains metadata about a result and it's originating statement.
 */
public interface ResultSummary
{
    /**
     * Number of nodes created by the statement that yielded this result.
     * @return number of nodes created
     */
    int nodesCreated();

    /**
     * Number of nodes deleted by the statement that yielded this result.
     * @return number of nodes deleted
     */
    int nodesDeleted();

    /**
     * Number of relationships created by the statement that yielded this result.
     * @return number of relationships created
     */
    int relationshipsCreated();

    /**
     * Number of relationships deleted by the statement that yielded this result.
     * @return number of relationships deleted
     */
    int relationshipsDeleted();

    /**
     * Number of properties set by the statement that yielded this result.
     * @return number of properties set
     */
    int propertiesSet();

    /**
     * Number of labels added to nodes by the statement that yielded this result.
     * @return number of labels added to nodes
     */
    int labelsAdded();

    /**
     * Number of labels removed from nodes by the statement that yielded this result.
     * @return number of labels removed from nodes
     */
    int labelsRemoved();

    /**
     * Number of indexes added by the statement that yielded this result.
     * @return number of indexes added
     */
    int indexesAdded();

    /**
     * Number of indexes removed by the statement that yielded this result.
     * @return number of indexes removed
     */
    int indexesRemoved();

    /**
     * Number of constraints added by the statement that yielded this result.
     * @return number of constraints added
     */
    int constraintsAdded();

    /**
     * Number of constraints removed by the statement that yielded this result.
     * @return number of constraints removed
     */
    int constraintsRemoved();

    /**
     * Check if the statement that yielded this result made updates to the database.
     * @return true if there were updates made by the statement
     */
    boolean containsUpdates();

    /**
     * The type of statement that yielded this result.
     * @return the type of statement that yielded this result
     */
    StatementType statementType();
}
