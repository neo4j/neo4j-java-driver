package neo4j.org.testkit.backend;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;

@Getter
public class TestkitState
{
    private final Map<String,Driver> drivers = new HashMap<>();
    private final Map<String, SessionState> sessionStates = new HashMap<>();
    private final Map<String,Result> results = new HashMap<>();
    private final Map<String,Transaction> transactions = new HashMap<>();
    private final Map<String,Neo4jException> errors = new HashMap<>();
    private int idGenerator = 0;

    public String newId()
    {
        return String.valueOf( idGenerator++ );
    }
}
