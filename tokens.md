# Token options

It would be nice to meet the following criteria:
1. Contain the usage of the type to its expected destination. 
For instance, a token with expiration is meant for the specific temporal manager implementation.
It should not be used elsewhere.
2. Avoid things like `token.withExpirationAt(timestamp).withSomeOtherCritea(criteria)`, 
especially mixed with the point above.
3. Keep `AuthTokens` factory concise.
4. Consider how we would go about supporting some other criteria for another auth token manager implementation 
that might need something else than the tempoval value.
