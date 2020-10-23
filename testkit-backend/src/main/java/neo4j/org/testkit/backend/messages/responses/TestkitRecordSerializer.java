package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

public class TestkitRecordSerializer extends StdSerializer<org.neo4j.driver.Record>
{

    protected TestkitRecordSerializer()
    {
        super( org.neo4j.driver.Record.class );
    }

    @Override
    public void serialize( Record record, JsonGenerator gen, SerializerProvider provider ) throws IOException
    {
        gen.writeStartArray();

        for ( Value value : record.values() )
        {
            gen.writeObject( value );
        }

        gen.writeEndArray();
    }
}
