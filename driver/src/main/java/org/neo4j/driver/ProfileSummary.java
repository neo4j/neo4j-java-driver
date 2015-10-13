package org.neo4j.driver;

import java.util.Map;

public interface ProfileSummary
{
    // TODO: long totalTime
    // TODO: long totalDbHits
    // TODO: long totalRows

    Map<String, Value> details();
}
