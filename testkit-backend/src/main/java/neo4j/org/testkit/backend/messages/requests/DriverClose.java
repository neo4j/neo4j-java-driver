package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
@NoArgsConstructor
public class DriverClose implements TestkitRequest
{
    private DriverCloseBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        testkitState.getDrivers().get( data.getDriverId() ).close();
        return Driver.builder().data( Driver.DriverBody.builder().id( data.getDriverId() ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    private static class DriverCloseBody
    {
        private String driverId;
    }
}
