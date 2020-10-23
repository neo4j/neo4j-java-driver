package neo4j.org.testkit.backend.messages.responses;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class Session implements TestkitResponse
{
    private SessionBody data;

    @Override
    public String testkitName()
    {
        return "Session";
    }

    @Setter
    @Getter
    @Builder
    public static class SessionBody
    {
        private String id;
    }
}
