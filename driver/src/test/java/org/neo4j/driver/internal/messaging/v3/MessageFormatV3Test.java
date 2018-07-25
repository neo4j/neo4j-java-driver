package org.neo4j.driver.internal.messaging.v3;

import org.junit.jupiter.api.Test;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.v2.MessageReaderV2;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

class MessageFormatV3Test
{
    @Test
    void shouldCreateCorrectWriter()
    {
        MessageFormatV3 format = new MessageFormatV3();

        MessageFormat.Writer writer = format.newWriter( mock( PackOutput.class ), true );

        assertThat( writer, instanceOf( MessageWriterV3.class ) );
    }

    @Test
    void shouldCreateCorrectReader()
    {
        MessageFormatV3 format = new MessageFormatV3();

        MessageFormat.Reader reader = format.newReader( mock( PackInput.class ) );

        assertThat( reader, instanceOf( MessageReaderV2.class ) );
    }
}
