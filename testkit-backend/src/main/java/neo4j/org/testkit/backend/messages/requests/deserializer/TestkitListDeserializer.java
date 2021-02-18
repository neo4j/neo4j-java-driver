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
package neo4j.org.testkit.backend.messages.requests.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static neo4j.org.testkit.backend.messages.responses.serializer.GenUtils.cypherTypeToJavaType;

public class TestkitListDeserializer extends StdDeserializer<List<?>>
{

    public TestkitListDeserializer()
    {
        super( List.class );
    }

    @Override
    public List<?> deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException, JsonProcessingException
    {
        List<Object> result = new ArrayList<>();

        JsonToken t = p.getCurrentToken();
        if ( t == JsonToken.END_OBJECT )
        {
            return result;
        }
        if ( t != JsonToken.START_ARRAY )
        {
            ctxt.reportWrongTokenException( this, JsonToken.FIELD_NAME, null );
        }

        JsonToken nextToken = p.nextToken();

        // standard list
        if ( nextToken == JsonToken.VALUE_STRING )
        {
            while ( nextToken != JsonToken.END_ARRAY && nextToken != null )
            {
                result.add( p.readValueAs( String.class ) );
                nextToken = p.nextToken();
            }
            return result;
        }

        //cypher parameter list
        while ( nextToken != JsonToken.END_ARRAY )
        {
            String paramType = null;

            if ( nextToken == JsonToken.START_OBJECT )
            {
                String fieldName = p.nextFieldName();
                if ( fieldName.equals( "name" ) )
                {
                    paramType = p.nextTextValue();
                    Class<?> mapValueType = cypherTypeToJavaType( paramType );
                    p.nextFieldName(); // next is data which we can drop
                    p.nextToken();
                    p.nextToken();
                    p.nextToken();
                    if ( mapValueType == null )
                    {
                        result.add( null );
                    }
                    else
                    {
                        result.add( p.readValueAs( mapValueType ) );
                    }
                }
            }
            nextToken = p.nextToken();
        }
        p.nextToken();
        p.nextToken();
        return result;
    }
}