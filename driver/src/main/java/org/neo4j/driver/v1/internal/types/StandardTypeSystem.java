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
 */
public class StandardTypeSystem implements TypeSystem
{
    public static StandardTypeSystem TYPE_SYSTEM = new StandardTypeSystem();

    private final TypeRepresentation anyType = constructType( ANY_TyCon );
    private final TypeRepresentation booleanType = constructType( BOOLEAN_TyCon );
    private final TypeRepresentation stringType = constructType( STRING_TyCon );
    private final TypeRepresentation numberType = constructType( NUMBER_TyCon );
    private final TypeRepresentation integerType = constructType( INTEGER_TyCon );
    private final TypeRepresentation floatType = constructType( FLOAT_TyCon );
    private final TypeRepresentation listType = constructType( LIST_TyCon );
    private final TypeRepresentation mapType = constructType( MAP_TyCon );
    private final TypeRepresentation identityType = constructType( IDENTITY_TyCon );
    private final TypeRepresentation nodeType = constructType( NODE_TyCon );
    private final TypeRepresentation relationshipType = constructType( RELATIONSHIP_TyCon );
    private final TypeRepresentation pathType = constructType( PATH_TyCon );
    private final TypeRepresentation nullType = constructType( NULL_TyCon );

    private StandardTypeSystem()
    {
    }

    /** the Cypher type ANY */
    @Override
    public Type ANY()
    {
        return anyType;
    }

    /** the Cypher type BOOLEAN */
    @Override
    public Type BOOLEAN()
    {
        return booleanType;
    }

    /** the Cypher type STRING */
    @Override
    public Type STRING()
    {
        return stringType;
    }

    /** the Cypher type NUMBER */
    @Override
    public Type NUMBER()
    {
        return numberType;
    }

    /** the Cypher type INTEGER */
    @Override
    public Type INTEGER()
    {
        return integerType;
    }

    /** the Cypher type FLOAT */
    @Override
    public Type FLOAT()
    {
        return floatType;
    }

    /** the Cypher type LIST */
    @Override
    public Type LIST()
    {
        return listType;
    }

    /** the Cypher type MAP */
    @Override
    public Type MAP()
    {
        return mapType;
    }

    /** the Cypher type IDENTITY */
    @Override
    public Type IDENTITY()
    {
        return identityType;
    }

    /** the Cypher type NODE */
    @Override
    public Type NODE()
    {
        return nodeType;
    }

    /** the Cypher type RELATIONSHIP */
    @Override
    public Type RELATIONSHIP()
    {
        return relationshipType;
    }

    /** the Cypher type PATH */
    @Override
    public Type PATH()
    {
        return pathType;
    }

    /** the Cypher type NULL */
    @Override
    public Type NULL()
    {
        return nullType;
    }

    private TypeRepresentation constructType( TypeConstructor tyCon )
    {
        return new TypeRepresentation( tyCon );
    }
}
