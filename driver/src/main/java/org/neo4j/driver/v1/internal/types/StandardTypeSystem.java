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
package org.neo4j.driver.v1.internal.types;

import org.neo4j.driver.v1.CoarseType;
import org.neo4j.driver.v1.Type;
import org.neo4j.driver.v1.TypeSystem;
import org.neo4j.driver.v1.Value;

import static org.neo4j.driver.v1.internal.types.TypeConstructor.ANY_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.BOOLEAN_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.FLOAT_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.IDENTITY_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.INTEGER_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.LIST_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.MAP_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.NODE_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.NULL_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.NUMBER_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.PATH_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.RELATIONSHIP_TyCon;
import static org.neo4j.driver.v1.internal.types.TypeConstructor.STRING_TyCon;

/**
 * Utility class for determining and working with the Cypher types of values
 *
 * @see Value
 * @see Type
 * @see CoarseType
 */
public final class StandardTypeSystem implements TypeSystem
{
    public static TypeSystem TYPE_SYSTEM = new StandardTypeSystem();

    private StandardTypeSystem()
    {
    }

    /** the Cypher type ANY */
    @Override
    public CoarseType ANY()
    {
        return constructType( ANY_TyCon );
    }

    /** the Cypher type BOOLEAN */
    @Override
    public CoarseType BOOLEAN()
    {
        return constructType( BOOLEAN_TyCon );
    }

    /** the Cypher type STRING */
    @Override
    public CoarseType STRING()
    {
        return constructType( STRING_TyCon );
    }

    /** the Cypher type NUMBER */
    @Override
    public CoarseType NUMBER()
    {
        return constructType( NUMBER_TyCon );
    }

    /** the Cypher type INTEGER */
    @Override
    public CoarseType INTEGER()
    {
        return constructType( INTEGER_TyCon );
    }

    /** the Cypher type FLOAT */
    @Override
    public CoarseType FLOAT()
    {
        return constructType( FLOAT_TyCon );
    }

    /** the Cypher type LIST */
    @Override
    public CoarseType LIST()
    {
        return constructType( LIST_TyCon );
    }

    /** the Cypher type MAP */
    @Override
    public CoarseType MAP()
    {
        return constructType( MAP_TyCon );
    }

    /** the Cypher type IDENTITY */
    @Override
    public CoarseType IDENTITY()
    {
        return constructType( IDENTITY_TyCon );
    }

    /** the Cypher type NODE */
    @Override
    public CoarseType NODE()
    {
        return constructType( NODE_TyCon );
    }

    /** the Cypher type RELATIONSHIP */
    @Override
    public CoarseType RELATIONSHIP()
    {
        return constructType( RELATIONSHIP_TyCon );
    }

    /** the Cypher type PATH */
    @Override
    public CoarseType PATH()
    {
        return constructType( PATH_TyCon );
    }

    /** the Cypher type NULL */
    @Override
    public CoarseType NULL()
    {
        return constructType( NULL_TyCon );
    }

    private TypeRepresentation constructType( TypeConstructor tyCon )
    {
        return new TypeRepresentation( tyCon );
    }
}
