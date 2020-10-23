package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class TestkitErrorResponse implements TestkitResponse
{
    @JsonProperty("msg")
    private String errorMessage;

    @Override
    public String testkitName()
    {
        return "Error";
    }
}
