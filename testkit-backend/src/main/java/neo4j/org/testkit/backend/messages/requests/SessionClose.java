package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
@NoArgsConstructor
public class SessionClose implements TestkitRequest
{
    private SessionCloseBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        testkitState.getSessionStates().get( data.getSessionId() ).getSession().close();
        return Session.builder().data( Session.SessionBody.builder().id( data.getSessionId() ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    private static class SessionCloseBody
    {
        private String sessionId;
    }
}
