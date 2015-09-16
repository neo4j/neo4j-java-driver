package org.neo4j.driver.internal;

import java.util.Arrays;

import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.spi.StreamCollector;

public class CombinedResultBuilder implements StreamCollector
{
    private final ResultBuilder resultBuilder;

    public CombinedResultBuilder( ResultBuilder resultBuilder )
    {
        this.resultBuilder = resultBuilder;
    }

    @Override
    public void fieldNames( String[] names )
    {
        // do nothing for now, but could be extended to handle EXPLAIN and PROFILE meta data in the future
    }

    @Override
    public void record( Value[] fields )
    {
        throw new IllegalStateException( "Not supported operation to add records " + Arrays.toString( fields ) );
    }

    public Result build()
    {
        return resultBuilder.build();
    }
}
