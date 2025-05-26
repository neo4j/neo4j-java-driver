/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.neo4j.driver.Value;

@Getter
@Builder
public class Summary implements TestkitResponse {
    private SummaryBody data;

    @Override
    public String testkitName() {
        return "Summary";
    }

    @Getter
    @Builder
    public static class SummaryBody {
        private ServerInfo serverInfo;

        private Counters counters;

        private Query query;

        private String database;

        private List<Notification> notifications;

        private List<GqlStatusObject> gqlStatusObjects;

        private Plan plan;

        private Profile profile;

        private String queryType;

        private Long resultAvailableAfter;

        private Long resultConsumedAfter;
    }

    @Getter
    @Builder
    public static class ServerInfo {
        private String address;

        private String protocolVersion;

        private String agent;
    }

    @Getter
    @Builder
    public static class Counters {
        private int constraintsAdded;

        private int constraintsRemoved;

        private boolean containsSystemUpdates;

        private boolean containsUpdates;

        private int indexesAdded;

        private int indexesRemoved;

        private int labelsAdded;

        private int labelsRemoved;

        private int nodesCreated;

        private int nodesDeleted;

        private int propertiesSet;

        private int relationshipsCreated;

        private int relationshipsDeleted;

        private int systemUpdates;
    }

    @Getter
    @Builder
    public static class Query {
        private String text;

        private Map<String, Value> parameters;
    }

    @Getter
    @Builder
    public static class Notification {
        private String code;

        private String title;

        private String description;

        private InputPosition position;

        private String severityLevel;

        private String rawSeverityLevel;

        private String category;

        private String rawCategory;
    }

    @Getter
    @Builder
    public static class GqlStatusObject {
        private String gqlStatus;

        private String statusDescription;

        private InputPosition position;

        private String severity;

        private String rawSeverity;

        private String classification;

        private String rawClassification;

        @JsonProperty("isNotification")
        private boolean notification;

        private Map<String, Value> diagnosticRecord;
    }

    @Getter
    @Builder
    public static class InputPosition {
        private int offset;

        private int line;

        private int column;
    }

    @Getter
    @SuperBuilder
    public static class Plan {
        private String operatorType;

        private Map<String, Object> args;

        private List<String> identifiers;

        private List<Plan> children;
    }

    @Getter
    @SuperBuilder
    public static class Profile extends Plan {
        private long dbHits;

        private long rows;

        private boolean hasPageCacheStats;

        private long pageCacheHits;

        private long pageCacheMisses;

        private double pageCacheHitRatio;

        private long time;
    }
}
