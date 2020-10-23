package neo4j.org.testkit.backend.messages;

import com.fasterxml.jackson.databind.module.SimpleModule;
import neo4j.org.testkit.backend.messages.responses.TestkitValueSerializer;

import org.neo4j.driver.Value;

public class TestkitModule extends SimpleModule
{
    public TestkitModule()
    {
        this.addSerializer( Value.class, new TestkitValueSerializer() );
    }
}
