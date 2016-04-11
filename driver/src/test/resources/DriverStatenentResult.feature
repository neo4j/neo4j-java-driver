@streaming_and_cursor_navigation @reset_database @in_dev
Feature: Statement Result Navigation

  This Feature is purposed to defined the uniform API of the Statement Result. What methods that are suppose to
  coexist in all the drivers and what is expected from them.

  Scenario: Get single value from single Statement Result should give a record
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    When running: MATCH (a:X) RETURN length((a)-[:T]->()) as length;
    Then using `Single` on `Statement Result` gives a `Record` containing:
      | length |
      | 3      |

  Scenario: Get single value from not single Statement Result should throw exception
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then using `Single` on `Statement Result` throws exception:
      | NoSuchRecordException                                                               |
      | Expected a result with a single record, but this result contains at least one more. |

  Scenario: Get single value from empty Statement Result should throw exception
    Given running: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    Then using `Single` on `Statement Result` throws exception:
      | NoSuchRecordException           |
      | Cannot retrieve a single record |

  Scenario: Get single value after iterating all but one record in Statement Result should not throw exception
    Given init: CREATE ({value: 2}), ({value: 2});
    When running: MATCH (n) RETURN n.value AS n;
    And using `Next` on `Statement Result` gives a `Record`
    Then using `Single` on `Statement Result` gives a `Record` containing:
      | n |
      | 2 |

  Scenario: Get single value after iterating while still multiple records in Statement Result should throw exception
    Given init: CREATE ({value: 2}), ({value: 2}), ({value: 2});
    When running: MATCH (n) RETURN n.value AS n;
    And using `Next` on `Statement Result` gives a `Record`
    Then using `Single` on `Statement Result` throws exception:
      | NoSuchRecordException                  |
      | Expected a result with a single record |

  Scenario: Get single value a second time throws exception
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    When running: MATCH (a:X) RETURN length((a)-[:T]->()) as length;
    Then using `Single` on `Statement Result` gives a `Record` containing:
      | length |
      | 3      |
    And using `Single` on `Statement Result` throws exception:
      | NoSuchRecordException           |
      | Cannot retrieve a single record |

  Scenario: Native support for iterating through Statement Result
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then iterating through the `Statement Result` should follow the native code pattern

  Scenario: Iterating through Statement Result
    Given init: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    When running: MATCH (a:X) RETURN length((a)-[:T]->()) as length;
    Then using `Peek` on `Statement Result` gives a `Record` containing:
      | length |
      | 3      |
    And using `Next` on `Statement Result` gives a `Record` containing:
      | length |
      | 3      |
    Then using `Peek` on `Statement Result` fails
    And using `Next` on `Statement Result` fails

  Scenario: Iterating through empty Statement Result
    Given running: MATCH (a:X) RETURN length((a)-[:T]->()) as length;
    Then using `Peek` on `Statement Result` fails
    And using `Next` on `Statement Result` fails

  Scenario: Iterating through Statement Result should be one directed
    Given init: CREATE ({value: 2}), ({value: 2});
    When running: MATCH (n) RETURN n.value AS n;
    And using `Next` on `Statement Result` gives a `Record`
    And using `Next` on `Statement Result` gives a `Record`
    Then it is not possible to go back

  Scenario: Get keys from Statement Result
    Given init: CREATE (a:A {value: 1})-[:REL {name: "r1"}]->(b:B {value: 2})-[:REL {name: "r2"}]->(c:C {value: 3})
    When running: MATCH (a)-[r {name:'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r<>r2 RETURN a,b,c
    Then using `Keys` on `Statement Result` gives:
      | keys |
      | a    |
      | b    |
      | c    |

  Scenario: Get keys from empty Statement Result
    Given running: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    Then using `Keys` on `Statement Result` gives:
      | keys |

  Scenario: Get list from Statement Result
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then using `List` on `Statement Result` gives:
      | n | m |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 2 | 1 |
      | 2 | 2 |
      | 2 | 3 |
      | 3 | 3 |
      | 3 | 1 |
      | 3 | 2 |

  Scenario: Get list from empty Statement Result
    Given running: CREATE (x:X), (x)-[:T]->(), (x)-[:T]->(), (x)-[:T]->(), (x)-[:AFFE]->();
    Then using `List` on `Statement Result` gives:
      |  |

  Scenario: List on Statement Result should give the remaining records
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    And using `Next` on `Statement Result` gives a `Record`
    And using `Next` on `Statement Result` gives a `Record`
    Then it is not possible to go back
    And using `List` on `Statement Result` gives a list of size 7, the previous records are lost

  Scenario: Get list from Statement Result multiple times gives emtpy list
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then using `List` on `Statement Result` gives:
      | n | m |
      | 1 | 1 |
      | 1 | 2 |
      | 1 | 3 |
      | 2 | 1 |
      | 2 | 2 |
      | 2 | 3 |
      | 3 | 3 |
      | 3 | 1 |
      | 3 | 2 |
    Then using `List` on `Statement Result` gives:
      |  |

  Scenario: Using `Consume` on `StatementResult` gives `ResultSummary`
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then using `Consume` on `StatementResult` gives `ResultSummary`

  Scenario: Using `Consume` on `StatementResult` gives `ResultSummary`
    Given init: CREATE ({value: 1}), ({value: 2}), ({value: 3});
    When running: MATCH (n), (m) RETURN n.value AS n, m.value AS m;
    Then using `Consume` on `StatementResult` multiple times gives the same `ResultSummary` each time

  Scenario: Record should contain key
    Given init: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When running: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then using `Keys` on the single record gives:
      | keys |
      | n1   |
      | n2   |

  Scenario: Record should contain value
    Given init: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When running: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then using `Values` on the single record gives:
      | values            |
      | (:A {"value": 1}) |
      | (:B {"value": 2}) |

  Scenario: Record should contain key and value and value should be retriveble by index
    Given init: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When running: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then using `Get` with index 0 on the single record gives:
      | value             |
      | (:A {"value": 1}) |

  Scenario: Record should contain key and value and value should be retriveble by key
    Given init: CREATE (a:A {value : 1})-[:KNOWS]->(b:B {value : 2})
    When running: MATCH (n1)-[rel:KNOWS]->(n2) RETURN n1, n2
    Then using `Get` with key `n2` on the single record gives:
      | value             |
      | (:B {"value": 2}) |