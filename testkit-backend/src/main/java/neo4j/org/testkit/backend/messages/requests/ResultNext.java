package neo4j.org.testkit.backend.messages.requests;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import neo4j.org.testkit.backend.TestkitState;
import neo4j.org.testkit.backend.messages.responses.Record;
import neo4j.org.testkit.backend.messages.responses.TestkitResponse;

@Setter
@Getter
@NoArgsConstructor
public class ResultNext implements TestkitRequest
{
    private ResultNextBody data;

    @Override
    public TestkitResponse process( TestkitState testkitState )
    {
        org.neo4j.driver.Record record = testkitState.getResults().get( data.getResultId() ).next();
        return Record.builder().data( Record.RecordBody.builder().values( record ).build() ).build();
    }

    @Setter
    @Getter
    @NoArgsConstructor
    public static class ResultNextBody
    {
        private String resultId;
    }
}
