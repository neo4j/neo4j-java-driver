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
import java.util.List;
import java.util.Map;
import java.lang.reflect.Array;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;


public class TestkitTypes
{

    public static String fromRecord( Record record )
    {
        String v = "";

        for ( Value value : record.values() )
        {
            if (v != "") {
                v += ",";
            }
            v += toTestkitType(value);
        }

        return v;
    }


    private static String toTestkitType( Object obj )
    {
        if ( obj instanceof Value) {
            Value value = (Value) obj;
            if ( value instanceof NullValue )
            {
                return "{\"data\" : null, \"name\" : \"CypherNull\"}";
            } else if ( value instanceof IntegerValue )
            {
                return toTestkitType(value.asInt());
            } else if ( value instanceof StringValue )
            {
                return toTestkitType(value.asString());
            } else if ( value instanceof ListValue )
            {
                return toTestkitType(((ListValue)value).asList());
            } else if ( value instanceof NodeValue )
            {
                Node node = ((NodeValue)value).asNode();
                String v = String.format("{\"id\":%s,\"labels\":%s,\"props\":%s}",
                    toTestkitType(node.id()), toTestkitType(node.labels()), toTestkitType(node.asMap()));
                return Testkit.wrap("Node", v);
            } else if ( value instanceof MapValue )
            {
                return "{\"data\" : {\"value\": {}, \"name\" : \"CypherMap\"}";
            }
        } else {
            if (obj instanceof Integer || obj instanceof Long) {
                return Testkit.wrap("CypherInt", Testkit.value(obj.toString()));
            } else if (obj instanceof String) {
                return Testkit.wrap("CypherString", Testkit.value("\""+obj.toString()+"\""));
            } else if (obj instanceof List) {
                List<?> list = (List<?>)obj;
                String v = "";
                for (int i = 0; i < list.size(); i++) {
                    if (i > 0) {
                        v += ",";
                    }
                    v += toTestkitType(list.get(i));
                }
                return Testkit.wrap("CypherList", Testkit.value("["+v+"]"));
            } else if (obj instanceof Map) {
                Map<String, ?> map = (Map<String, ?>)obj;
                String v = "";

                for (Map.Entry<String, ?> entry : map.entrySet()) {
                    if (v != "") {
                        v += ",";
                    }
                    v += String.format("\"%s\":%s", entry.getKey(), toTestkitType(entry.getValue()));
                }
                return Testkit.wrap("CypherMap", Testkit.value("{"+v+"}"));
            }
        }

        throw new RuntimeException("Can not convert to testkit type:"+obj.getClass());
    }
}
