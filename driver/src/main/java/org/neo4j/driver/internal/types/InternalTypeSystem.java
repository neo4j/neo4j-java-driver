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
package org.neo4j.driver.internal.types;

import org.neo4j.driver.Value;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;

import static org.neo4j.driver.internal.types.TypeConstructor.ANY;
import static org.neo4j.driver.internal.types.TypeConstructor.BOOLEAN;
import static org.neo4j.driver.internal.types.TypeConstructor.BYTES;
import static org.neo4j.driver.internal.types.TypeConstructor.DATE;
import static org.neo4j.driver.internal.types.TypeConstructor.DATE_TIME;
import static org.neo4j.driver.internal.types.TypeConstructor.DURATION;
import static org.neo4j.driver.internal.types.TypeConstructor.FLOAT;
import static org.neo4j.driver.internal.types.TypeConstructor.INTEGER;
import static org.neo4j.driver.internal.types.TypeConstructor.LIST;
import static org.neo4j.driver.internal.types.TypeConstructor.LOCAL_DATE_TIME;
import static org.neo4j.driver.internal.types.TypeConstructor.LOCAL_TIME;
import static org.neo4j.driver.internal.types.TypeConstructor.MAP;
import static org.neo4j.driver.internal.types.TypeConstructor.NODE;
import static org.neo4j.driver.internal.types.TypeConstructor.NULL;
import static org.neo4j.driver.internal.types.TypeConstructor.NUMBER;
import static org.neo4j.driver.internal.types.TypeConstructor.PATH;
import static org.neo4j.driver.internal.types.TypeConstructor.POINT;
import static org.neo4j.driver.internal.types.TypeConstructor.RELATIONSHIP;
import static org.neo4j.driver.internal.types.TypeConstructor.STRING;
import static org.neo4j.driver.internal.types.TypeConstructor.TIME;

/**
 * Utility class for determining and working with the Cypher types of values
 *
 * @see Value
 * @see Type
 */
public class InternalTypeSystem implements TypeSystem
{
    public static InternalTypeSystem TYPE_SYSTEM = new InternalTypeSystem();

    private final TypeRepresentation anyType = constructType( ANY );
    private final TypeRepresentation booleanType = constructType( BOOLEAN );
    private final TypeRepresentation bytesType = constructType( BYTES );
    private final TypeRepresentation stringType = constructType( STRING );
    private final TypeRepresentation numberType = constructType( NUMBER );
    private final TypeRepresentation integerType = constructType( INTEGER );
    private final TypeRepresentation floatType = constructType( FLOAT );
    private final TypeRepresentation listType = constructType( LIST );
    private final TypeRepresentation mapType = constructType( MAP );
    private final TypeRepresentation nodeType = constructType( NODE );
    private final TypeRepresentation relationshipType = constructType( RELATIONSHIP );
    private final TypeRepresentation pathType = constructType( PATH );
    private final TypeRepresentation pointType = constructType( POINT );
    private final TypeRepresentation dateType = constructType( DATE );
    private final TypeRepresentation timeType = constructType( TIME );
    private final TypeRepresentation localTimeType = constructType( LOCAL_TIME );
    private final TypeRepresentation localDateTimeType = constructType( LOCAL_DATE_TIME );
    private final TypeRepresentation dateTimeType = constructType( DATE_TIME );
    private final TypeRepresentation durationType = constructType( DURATION );
    private final TypeRepresentation nullType = constructType( NULL );

    private InternalTypeSystem()
    {
    }

    @Override
    public Type ANY()
    {
        return anyType;
    }

    @Override
    public Type BOOLEAN()
    {
        return booleanType;
    }

    @Override
    public Type BYTES()
    {
        return bytesType;
    }

    @Override
    public Type STRING()
    {
        return stringType;
    }

    @Override
    public Type NUMBER()
    {
        return numberType;
    }

    @Override
    public Type INTEGER()
    {
        return integerType;
    }

    @Override
    public Type FLOAT()
    {
        return floatType;
    }

    @Override
    public Type LIST()
    {
        return listType;
    }

    @Override
    public Type MAP()
    {
        return mapType;
    }

    @Override
    public Type NODE()
    {
        return nodeType;
    }

    @Override
    public Type RELATIONSHIP()
    {
        return relationshipType;
    }

    @Override
    public Type PATH()
    {
        return pathType;
    }

    @Override
    public Type POINT()
    {
        return pointType;
    }

    @Override
    public Type DATE()
    {
        return dateType;
    }

    @Override
    public Type TIME()
    {
        return timeType;
    }

    @Override
    public Type LOCAL_TIME()
    {
        return localTimeType;
    }

    @Override
    public Type LOCAL_DATE_TIME()
    {
        return localDateTimeType;
    }

    @Override
    public Type DATE_TIME()
    {
        return dateTimeType;
    }

    @Override
    public Type DURATION()
    {
        return durationType;
    }

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
