package neo4j.org.testkit.backend.messages.requests;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="name")
@JsonSubTypes ( { @JsonSubTypes.Type( NewDriver.class ), @JsonSubTypes.Type( NewSession.class ),
@JsonSubTypes.Type( SessionRun.class ), @JsonSubTypes.Type( ResultNext.class ),
@JsonSubTypes.Type( SessionClose.class ), @JsonSubTypes.Type( DriverClose.class )} )
public interface TestkitRequest
{
    TestkitResponse process( TestkitState testkitState );
}
