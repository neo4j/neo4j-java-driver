package neo4j.org.testkit.backend.messages.requests;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.TestkitCypherTypeDeserializer;
import neo4j.org.testkit.backend.messages.responses.Result;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import java.util.Map;

import org.neo4j.driver.Query;
import org.neo4j.driver.Session;

@Setter
@Getter
@NoArgsConstructor
public class SessionRun implements TestkitRequest
{
    private SessionRunBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Session session = testkitState.getSessionStates().get( data.getSessionId() ).getSession();
        Query query = new Query( data.getCypher(), data.getParams() );
        org.neo4j.driver.Result result = session.run( query );
        String newId = testkitState.newId();
        testkitState.getResults().put( newId, result );

        return Result.builder().data( Result.ResultBody.builder().id( newId ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class SessionRunBody
    {
        private String sessionId;
        private String cypher;

        @JsonDeserialize( using = TestkitCypherTypeDeserializer.class )
        private Map<String,Object> params;
        private String txMeta;
        private int timeout;
    }
}
