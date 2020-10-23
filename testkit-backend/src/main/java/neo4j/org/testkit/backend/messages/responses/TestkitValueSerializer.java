package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import org.neo4j.driver.Value;
import org.neo4j.driver.internal.types.InternalTypeSystem;

public class TestkitValueSerializer extends StdSerializer<Value>
{
    public TestkitValueSerializer( )
    {
        super( Value.class );
    }

    @Override
    public void serialize( Value value, JsonGenerator gen, SerializerProvider provider ) throws IOException
    {
        gen.writeStartObject();

        if ( InternalTypeSystem.TYPE_SYSTEM.BOOLEAN().isTypeOf( value ) )
        {
            gen.writeStringField( "name", "CypherBool" );
            gen.writeFieldName( "data" );
            gen.writeStartObject();
            gen.writeFieldName( "value" );
            gen.writeObject( value.asBoolean() );
            gen.writeEndObject();
        } else if ( InternalTypeSystem.TYPE_SYSTEM.NULL().isTypeOf( value ) ) {
            gen.writeStringField( "name", "CypherNull" );
            gen.writeFieldName( "data" );
            gen.writeStartObject();
            gen.writeFieldName( "value" );
            gen.writeNull();
            gen.writeEndObject();
        } else if ( InternalTypeSystem.TYPE_SYSTEM.INTEGER().isTypeOf( value ) ) {
            gen.writeStringField( "name", "CypherInt" );
            gen.writeFieldName( "data" );
            gen.writeStartObject();
            gen.writeFieldName( "value" );
            gen.writeObject( value.asInt() );
            gen.writeEndObject();
        } else if ( InternalTypeSystem.TYPE_SYSTEM.FLOAT().isTypeOf( value ) ) {
            gen.writeStringField( "name", "CypherFloat" );
            gen.writeFieldName( "data" );
            gen.writeStartObject();
            gen.writeFieldName( "value" );
            gen.writeObject( value.asDouble() );
            gen.writeEndObject();
        } else if ( InternalTypeSystem.TYPE_SYSTEM.STRING().isTypeOf( value ) ) {
            gen.writeStringField( "name", "CypherString" );
            gen.writeFieldName( "data" );
            gen.writeStartObject();
            gen.writeFieldName( "value" );
            gen.writeObject( value.asString() );
            gen.writeEndObject();
        }


        gen.writeEndObject();

    }
}
