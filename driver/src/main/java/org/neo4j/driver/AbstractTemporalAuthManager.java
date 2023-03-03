package org.neo4j.driver;

import java.util.concurrent.CompletionStage;

// option 3
// contains details quite well, but requires user to extend this class
// no need for AuthTokenManagers
// a bit less flexible
// at present AuthTokenManagers actually supports creating the implementation with different arguments, for instance it
// accepts Supplier<token data> as a convenience for the sync API and ensures it is scheduled on non-driver thread
public abstract class AbstractTemporalAuthManager implements AuthTokenManager {

    protected abstract CompletionStage<AuthData> createNewTokenStage();

    @Override
    public CompletionStage<AuthToken> getToken() {
        // TODO
        var stage = createNewTokenStage();
        // TODO
        return null;
    }

    @Override
    public void onExpired(AuthToken authToken) {
        // TODO
    }

    protected static class AuthData {
        public AuthData() {}
    }
}
