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
package org.neo4j.driver.integration;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.Values.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import org.neo4j.driver.testutil.ParallelizableIT;
import org.neo4j.driver.testutil.SessionExtension;

@ParallelizableIT
class ResultStreamIT {
    @RegisterExtension
    static final SessionExtension session = new SessionExtension();

    @Test
    void shouldAllowIteratingOverResultStream() {
        // When
        var res = session.run("UNWIND [1,2,3,4] AS a RETURN a");

        // Then I should be able to iterate over the result
        var idx = 1;
        while (res.hasNext()) {
            assertEquals(idx++, res.next().get("a").asLong());
        }
    }

    @Test
    void shouldHaveFieldNamesInResult() {
        // When
        var res = session.run("CREATE (n:TestNode {name:'test'}) RETURN n");

        // Then
        assertEquals("[n]", res.keys().toString());
        assertNotNull(res.single());
        assertEquals("[n]", res.keys().toString());
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingField() {
        // Given
        var rs = session.run("CREATE (n:Person {name:$name}) RETURN n", parameters("name", "Tom Hanks"));

        // When
        var single = rs.single();

        // Then
        assertTrue(single.get("m").isNull());
    }

    @Test
    void shouldGiveHelpfulFailureMessageWhenAccessNonExistingPropertyOnNode() {
        // Given
        var rs = session.run("CREATE (n:Person {name:$name}) RETURN n", parameters("name", "Tom Hanks"));

        // When
        var record = rs.single();

        // Then
        assertTrue(record.get("n").get("age").isNull());
    }

    @Test
    void shouldNotReturnNullKeysOnEmptyResult() {
        // Given
        var rs = session.run("CREATE (n:Person {name:$name})", parameters("name", "Tom Hanks"));

        // THEN
        assertNotNull(rs.keys());
    }

    @Test
    void shouldBeAbleToReuseSessionAfterFailure() {
        // Given
        assertThrows(Exception.class, () -> session.run("INVALID"));

        // When
        var res2 = session.run("RETURN 1");

        // Then
        assertTrue(res2.hasNext());
        assertEquals(res2.next().get("1").asLong(), 1L);
    }

    @Test
    void shouldBeAbleToAccessSummaryAfterTransactionFailure() {
        var resultRef = new AtomicReference<Result>();

        assertThrows(ClientException.class, () -> {
            try (var tx = session.beginTransaction()) {
                var result = tx.run("UNWIND [1,2,0] AS x RETURN 10/x");
                resultRef.set(result);
                tx.commit();
            }
        });

        var result = resultRef.get();
        assertNotNull(result);
        assertEquals(0, result.consume().counters().nodesCreated());
    }

    @Test
    void shouldConvertEmptyResultToStream() {
        var count = session.run("MATCH (n:WrongLabel) RETURN n").stream().count();

        assertEquals(0, count);

        var anyRecord =
                session.run("MATCH (n:OtherWrongLabel) RETURN n").stream().findAny();

        assertFalse(anyRecord.isPresent());
    }

    @Test
    void shouldConvertResultToStream() {
        var receivedList = session.run("UNWIND range(1, 10) AS x RETURN x").stream()
                .map(record -> record.get(0))
                .map(Value::asInt)
                .collect(toList());

        assertEquals(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), receivedList);
    }

    @Test
    void shouldConvertImmediatelyFailingResultToStream() {
        List<Integer> seen = new ArrayList<>();

        var e = assertThrows(ClientException.class, () -> session.run("RETURN 10 / 0").stream()
                .forEach(record -> seen.add(record.get(0).asInt())));

        assertThat(e.getMessage(), containsString("/ by zero"));

        assertEquals(emptyList(), seen);
    }

    @Test
    void shouldConvertEventuallyFailingResultToStream() {
        List<Integer> seen = new ArrayList<>();

        var e = assertThrows(
                ClientException.class,
                () -> session.run("CYPHER runtime=interpreted UNWIND range(5, 0, -1) AS x RETURN x / x").stream()
                        .forEach(record -> seen.add(record.get(0).asInt())));

        assertThat(e.getMessage(), containsString("/ by zero"));

        // stream should manage to summary all elements except the last one, which produces an error
        assertEquals(asList(1, 1, 1, 1, 1), seen);
    }

    @Test
    void shouldEmptyResultWhenConvertedToStream() {
        var result = session.run("UNWIND range(1, 10) AS x RETURN x");

        assertTrue(result.hasNext());
        assertEquals(1, result.next().get(0).asInt());

        assertTrue(result.hasNext());
        assertEquals(2, result.next().get(0).asInt());

        var list = result.stream().map(record -> record.get(0).asInt()).collect(toList());
        assertEquals(asList(3, 4, 5, 6, 7, 8, 9, 10), list);

        assertFalse(result.hasNext());
        assertThrows(NoSuchRecordException.class, result::next);
        assertEquals(emptyList(), result.list());
        assertEquals(0, result.stream().count());
    }

    @Test
    void shouldConsumeLargeResultAsParallelStream() {
        var receivedList = session.run("UNWIND range(1, 200000) AS x RETURN 'value-' + x").stream()
                .parallel()
                .map(record -> record.get(0))
                .map(Value::asString)
                .collect(toList());

        var expectedList =
                IntStream.range(1, 200001).mapToObj(i -> "value-" + i).collect(toList());

        assertEquals(expectedList, receivedList);
    }
}
