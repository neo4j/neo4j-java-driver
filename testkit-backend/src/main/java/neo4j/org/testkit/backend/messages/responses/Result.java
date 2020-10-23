package neo4j.org.testkit.backend.messages.responses;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class Result implements TestkitResponse
{
    private ResultBody data;

    @Override
    public String testkitName()
    {
        return "Result";
    }

    @Setter
    @Getter
    @Builder
    public static class ResultBody
    {
        private String id;
    }
}
