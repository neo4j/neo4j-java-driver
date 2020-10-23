
package neo4j.org.testkit.backend.messages;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestkitCypherTypeDeserializer extends StdDeserializer<Map<String,Object>>
{
    public TestkitCypherTypeDeserializer()
    {
        super( Map.class );
    }

    public TestkitCypherTypeDeserializer( Class<Map> typeClass )
    {
        super( typeClass );
    }

    @Override
    public Map<String,Object> deserialize( JsonParser p, DeserializationContext ctxt ) throws IOException, JsonProcessingException
    {
        Map<String,Object> result = new HashMap<>();

        String key;
        if ( p.isExpectedStartObjectToken() )
        {
            key = p.nextFieldName();
        }
        else
        {
            JsonToken t = p.getCurrentToken();
            if ( t == JsonToken.END_OBJECT )
            {
                return Collections.emptyMap();
            }
            if ( t != JsonToken.FIELD_NAME )
            {
                ctxt.reportWrongTokenException( this, JsonToken.FIELD_NAME, null );
            }
            key = p.getCurrentName();
        }

        for ( ; key != null; key = p.nextFieldName() )
        {
            String paramType = null;

            if ( p.nextToken() == JsonToken.START_OBJECT )
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
                        result.put( key, null );
                    } else
                    {
                        result.put( key, p.readValueAs( mapValueType ) );
                    }

                }
            }

        }
        return result;
    }

    private Class<?> cypherTypeToJavaType( String typeString )
    {
        switch ( typeString )
        {
        case "CypherBool":
            return Boolean.class;
        case "CypherInt":
            return Integer.class;
        case "CypherFloat":
            return Double.class;
        case "CypherString":
            return String.class;
        case "CypherList":
            return List.class;
        case "CypherMap":
            return Map.class;
        case "CypherNull":
            return null;
        default:
            return null;
        }
    }
}

