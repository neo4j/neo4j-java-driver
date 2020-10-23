package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.SessionState;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Session;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;

@Setter
@Getter
@NoArgsConstructor
public class NewSession implements TestkitRequest
{
    private NewSessionBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        Driver driver = testkitState.getDrivers().get( data.getDriverId() );
        AccessMode formattedAccessMode = data.getAccessMode().equals( "r" ) ? AccessMode.READ : AccessMode.WRITE;
        //Bookmark bookmark = InternalBookmark.parse(  requestData.get("bookmarks").asText() );
        org.neo4j.driver.Session session = driver.session( SessionConfig.builder()
                                                                        .withDefaultAccessMode( formattedAccessMode ).build() );
        String newId = testkitState.newId();
        testkitState.getSessionStates().put( newId, new SessionState( session ) );

        return Session.builder().data( Session.SessionBody.builder().id( newId ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class NewSessionBody
    {
        private String driverId;
        private String accessMode;
        private String bookmarks;
        private String database;
        private int fetchSize;
    }
}
