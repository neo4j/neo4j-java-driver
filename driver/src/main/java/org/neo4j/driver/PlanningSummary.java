package org.neo4j.driver;

import java.util.Map;

public interface PlanningSummary
{
    // TODO:String cypherVersion()

    /**
     * @return type of statement that has been executed
     */
    StatementType statementType();

    // TODO: All preparser options should go here
    Map<String,Value> details();
}
