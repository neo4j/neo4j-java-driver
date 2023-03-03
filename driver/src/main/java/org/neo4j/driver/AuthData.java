package org.neo4j.driver;

public interface AuthData<T> {
    AuthToken authToken();

    T metaData();
}
