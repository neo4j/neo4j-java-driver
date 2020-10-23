package neo4j.org.testkit.backend.messages.responses;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class Driver implements TestkitResponse
{
    private final DriverBody data;

    @Override
    public String testkitName()
    {
        return "Driver";
    }

    @Setter
    @Getter
    @Builder
    public static class DriverBody
    {
        private final String id;
    }
}
