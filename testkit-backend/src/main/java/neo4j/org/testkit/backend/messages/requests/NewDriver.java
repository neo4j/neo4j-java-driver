package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Driver;
import neo4j.org.testkit.backend.messages.responses.TestkitErrorResponse;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;

@Setter
@Getter
@NoArgsConstructor
public class NewDriver implements TestkitRequest
{
    private NewDriverBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        String id = testkitState.newId();

        AuthToken authToken;
        switch ( data.getAuthorizationToken().getTokens().get( "scheme" ) )
        {
        case "basic":
            authToken = AuthTokens.basic( data.authorizationToken.getTokens().get( "principal" ),
                                          data.authorizationToken.getTokens().get( "credentials" ),
                                          data.authorizationToken.getTokens().get( "realm" ) );
            break;
        default:
            return TestkitErrorResponse.builder().errorMessage( "Auth scheme " + data.authorizationToken.getTokens().get( "scheme" ) + "not implemented" ).build();
        }

        testkitState.getDrivers().putIfAbsent( id, GraphDatabase.driver( data.uri, authToken ) );
        return Driver.builder().data( Driver.DriverBody.builder().id( id ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class NewDriverBody
    {
        private String uri;
        private AuthorizationToken authorizationToken;
        private String userAgent;
    }
}
