package org.neo4j.driver.v1;

public class RetryLogic
{
    public static final RetryLogic TRY_UP_TO_3_TIMES_WITH_10_SECOND_PAUSE = new RetryLogic( 3, 10_000 );

    private final int attempts;
    private final int pause;

    public RetryLogic( int attempts, int pause )
    {
        this.attempts = attempts;
        this.pause = pause;
    }

    public int attempts()
    {
        return attempts;
    }

    public int pause()
    {
        return pause;
    }
}
