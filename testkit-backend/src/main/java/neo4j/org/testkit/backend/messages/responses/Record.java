package neo4j.org.testkit.backend.messages.responses;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@Builder
public class Record implements TestkitResponse
{
    private RecordBody data;

    @Override
    public String testkitName()
    {
        return "Record";
    }

    @Setter
    @Getter
    @Builder
    public static class RecordBody
    {
        @JsonSerialize( using = TestkitRecordSerializer.class )
        private org.neo4j.driver.Record values;
    }
}
