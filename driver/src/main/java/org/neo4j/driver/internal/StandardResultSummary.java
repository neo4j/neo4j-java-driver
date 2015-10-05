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
package org.neo4j.driver.internal;

import org.neo4j.driver.ResultSummary;
import org.neo4j.driver.StatementType;

public class StandardResultSummary implements ResultSummary
{
    private final int nodesCreated;
    private final int nodesDeleted;
    private final int relationshipsCreated;
    private final int relationshipsDeleted;
    private final int propertiesSet;
    private final int labelsAdded;
    private final int labelsRemoved;
    private final int indexesAdded;
    private final int indexesRemoved;
    private final int constraintsAdded;
    private final int constraintsRemoved;
    private final StatementType statementType;

    public StandardResultSummary(
            int nodesCreated,
            int nodesDeleted,
            int relationshipsCreated,
            int relationshipsDeleted,
            int propertiesSet,
            int labelsAdded,
            int labelsRemoved,
            int indexesAdded,
            int indexesRemoved,
            int constraintsAdded,
            int constraintsRemoved,
            StatementType statementType )
    {
        this.nodesCreated = nodesCreated;
        this.nodesDeleted = nodesDeleted;
        this.relationshipsCreated = relationshipsCreated;
        this.relationshipsDeleted = relationshipsDeleted;
        this.propertiesSet = propertiesSet;
        this.labelsAdded = labelsAdded;
        this.labelsRemoved = labelsRemoved;
        this.indexesAdded = indexesAdded;
        this.indexesRemoved = indexesRemoved;
        this.constraintsAdded = constraintsAdded;
        this.constraintsRemoved = constraintsRemoved;
        this.statementType = statementType;
    }

    @Override
    public int nodesCreated()
    {
        return nodesCreated;
    }

    @Override
    public int nodesDeleted()
    {
        return nodesDeleted;
    }

    @Override
    public int relationshipsCreated()
    {
        return relationshipsCreated;
    }

    @Override
    public int relationshipsDeleted()
    {
        return relationshipsDeleted;
    }

    @Override
    public int propertiesSet()
    {
        return propertiesSet;
    }

    @Override
    public int labelsAdded()
    {
        return labelsAdded;
    }

    @Override
    public int labelsRemoved()
    {
        return labelsRemoved;
    }

    @Override
    public int indexesAdded()
    {
        return indexesAdded;
    }

    @Override
    public int indexesRemoved()
    {
        return indexesRemoved;
    }

    @Override
    public int constraintsAdded()
    {
        return constraintsAdded;
    }

    @Override
    public int constraintsRemoved()
    {
        return constraintsRemoved;
    }

    @Override
    public boolean containsUpdates()
    {
        return (nodesCreated +
               nodesDeleted +
               relationshipsCreated +
               relationshipsDeleted +
               propertiesSet +
               labelsAdded +
               labelsRemoved +
               indexesAdded +
               indexesRemoved +
               constraintsAdded +
               constraintsRemoved) > 0;
    }

    @Override
    public StatementType statementType()
    {
        return statementType;
    }
}
