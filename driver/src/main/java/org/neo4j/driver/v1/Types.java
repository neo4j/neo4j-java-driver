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
package org.neo4j.driver.v1;

import org.neo4j.driver.v1.internal.types.TypeConstructor;
import org.neo4j.driver.v1.internal.types.TypeRepresentation;

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
public final class Types
{
    private Types() {
        throw new UnsupportedOperationException(  );
    }

    /** the Cypher type ANY */
    public static CoarseType ANY = constructType( ANY_TyCon );

    /** the Cypher type BOOLEAN */
    public static CoarseType BOOLEAN = constructType( BOOLEAN_TyCon );

    /** the Cypher type STRING */
    public static CoarseType STRING = constructType( STRING_TyCon );

    /** the Cypher type NUMBER */
    public static CoarseType NUMBER = constructType( NUMBER_TyCon );

    /** the Cypher type INTEGER */
    public static CoarseType INTEGER = constructType( INTEGER_TyCon );

    /** the Cypher type FLOAT */
    public static CoarseType FLOAT = constructType( FLOAT_TyCon );

    /** the Cypher type LIST */
    public static CoarseType LIST = constructType( LIST_TyCon );

    /** the Cypher type MAP */
    public static CoarseType MAP = constructType( MAP_TyCon );

    /** the Cypher type IDENTITY */
    public static CoarseType IDENTITY = constructType( IDENTITY_TyCon );

    /** the Cypher type NODE */
    public static CoarseType NODE = constructType( NODE_TyCon );

    /** the Cypher type RELATIONSHIP */
    public static CoarseType RELATIONSHIP = constructType( RELATIONSHIP_TyCon );

    /** the Cypher type PATH */
    public static CoarseType PATH = constructType( PATH_TyCon );

    /** the Cypher type NULL */
    public static CoarseType NULL = constructType( NULL_TyCon );

    private static TypeRepresentation constructType( TypeConstructor tyCon )
    {
        return new TypeRepresentation( tyCon );
    }
}
