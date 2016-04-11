@error_reporting
Feature: Error Reporting

  Scenario: Running a session before closing transaction should give exception
    Given I have a driver
    When I start a `Transaction` through a session
    And `run` a query with that same session without closing the transaction first
    Then it throws a `ClientException`
      | start of message                                                                                                       |
      | Please close the currently open transaction object before running more statements/transactions in the current session. |

  Scenario: Beginning a new transaction before closing the previous should give exception
    Given I have a driver
    When I start a `Transaction` through a session
    And I start a new `Transaction` with the same session before closing the previous
    Then it throws a `ClientException`
      | start of message                                                                                                       |
      | Please close the currently open transaction object before running more statements/transactions in the current session. |

  Scenario: Misspelled cypher statement should give exception
    Given I have a driver
    When I run a non valid cypher statement
    Then it throws a `ClientException`
      | start of message |
      | Invalid input    |

  Scenario: Trying to connect a driver to the wrong port gives exception
    When I set up a driver to an incorrect port
    Then it throws a `ClientException`
      | start of message     |
      | Unable to connect to |

  Scenario: Trying to connect a driver to the wrong scheme gives exception
    When I set up a driver with wrong scheme
    Then it throws a `ClientException`
      | start of message       |
      | Unsupported transport: |

  @fixed_session_pool
  Scenario: Running out of sessions should give exception
    Given I have a driver with fixed pool size of 1
    And I store a session
    When I try to get a session
    Then it throws a `ClientException`
      | start of message                                                                                                             |
      | Failed to acquire a session with Neo4j as all the connections in the connection pool are already occupied by other sessions. |

  @fixed_session_pool
  Scenario: Reusing session then running out of sessions should give exception
    Given I have a driver with fixed pool size of 1
    When I start a `Transaction` through a session
    And I close the session
    And I store a session
    Then I get no exception
    And I try to get a session
    And it throws a `ClientException`
      | start of message                                                                                                             |
      | Failed to acquire a session with Neo4j as all the connections in the connection pool are already occupied by other sessions. |