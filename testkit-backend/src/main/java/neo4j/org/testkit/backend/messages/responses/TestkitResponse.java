package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="name")
public interface TestkitResponse
{
    String testkitName();
}
