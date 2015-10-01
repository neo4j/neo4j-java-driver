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
 * Denotes the type of statement executed. This can be helpful to perform introspection on a statement. You can
 * couple this metadata with the `EXPLAIN` statement prefix to find out what effect a query will have without executing
 * it.
 */
public enum StatementType
{
    /** A read/write query, that creates or updates data, and also produces a result. */
    READ_WRITE,

    /** A write-only query, that creates or updates data, but does not yield any result records. */
    WRITE_ONLY,

    /**
     * A schema changing query, that updates the schema but neither changes any data nor yields any rows in the
     * result.
     */
    SCHEMA_WRITE,

    /** A read-only query, that does not change any data, but only produces a result. */
    READ_ONLY


}
