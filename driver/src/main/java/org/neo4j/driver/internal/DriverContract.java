package org.neo4j.driver.internal;

import org.neo4j.driver.v1.RetryLogic;

/**
 * The set of Driver behaviours that can be tuned by a client
 * application, such as timeouts, retry logic, etc.
 */
public class DriverContract
{
    private final RetryLogic retryLogic;

    public DriverContract( RetryLogic retryLogic )
    {
        this.retryLogic = retryLogic;
    }

    public RetryLogic retryLogic()
    {
        return retryLogic;
    }
}
