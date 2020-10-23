package neo4j.org.testkit.backend.messages.responses;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class TeskitBackendErrorResponse implements TestkitResponse
{
    @Override
    public String testkitName()
    {
        return "BackendError";
    }
}
