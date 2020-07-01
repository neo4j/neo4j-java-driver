/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.Collections;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;

public class NutkitTypes
{

    public static String fromRecord( Record record )
    {
        StringBuilder stringBuilder = new StringBuilder();

        for ( Value value : record.values() )
        {
            stringBuilder.append( toNutkitType( value ) );
        }

        return stringBuilder.toString();
    }

    private static String toNutkitType( Value value )
    {
        if ( value instanceof NullValue )
        {
            return "{\"data\" : null, \"name\" : \"CypherNull\"}";
        } else if ( value instanceof IntegerValue )
        {
            return "{\"data\" : {\"value\": " + value.asInt() + "}, \"name\" : \"CypherInt\"}";
        } else if ( value instanceof StringValue )
        {
            return "{\"data\" : {\"value\": \"" + value.asString() + "\"}, \"name\" : \"CypherString\"}";
        } else if ( value instanceof ListValue )
        {
            ListValue listValue = ( ListValue ) value;
            return "{\"data\" : {\"value\": [" + listValue.values( NutkitTypes::toNutkitType ) + "]}, \"name\" : \"CypherList\"}";
        } else if ( value instanceof NodeValue )
        {
            NodeValue node = ( NodeValue ) value;
            return "{\"data\" : {" +
                   "\"labels\" : " + new ListValue(  ) + "," +
                   "\"props\" : " + new MapValue( Collections.emptyMap() ) + "," +
                   "\"id\": " + toNutkitType( new IntegerValue( node.asNode().id() )) + "}," +
                   "\"name\" : \"Node\"}";
        } else if ( value instanceof MapValue )
        {
            return "{\"data\" : {\"value\": {}, \"name\" : \"CypherMap\"}";
        }

        return "";
    }
}
