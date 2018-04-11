Feature: Obligation state lifecycle

  Scenario: New obligation creates a state event
    When the following obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 20       | 10     |
    Then the following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |
    And the obligation state store should contain:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |

  Scenario: Updated obligation overwrites existing one
    Given the following obligations:
      | id | security | quantity | amount |
      | 2  | abc      | 20       | 10     |
    When the following obligations:
      | id | security | quantity | amount |
      | 2  | abc      | 50       | 50     |
    Then the following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 2  | OPEN   | 20.0         | 10.0       |
      | 2  | OPEN   | 50.0         | 50.0       |
    And the obligation state store should contain:
      | id | status | openQuantity | openAmount |
      | 2  | OPEN   | 50.0         | 50.0       |

  Scenario: Apply confirmation to obligation state
    Given the following obligations:
      | id | security | quantity | amount |
      | 4  | first    | 200      | 100    |
      | 5  | second   | 20       | 10     |
    And the following obligation states are published:
      | id | openQuantity | openAmount | status |
      | 4  | 200.0        | 100.0      | OPEN   |
      | 5  | 20.0         | 10.0       | OPEN   |
    When the following confirmations are received:
      | id | obligationId | quantity |
      | A  | 4            | 150      |
      | B  | 4            | 50       |
      | C  | 5            | 20       |
    Then the following obligation states are published:
      | id | status            | openQuantity |
      | 4  | PARTIALLY_SETTLED | 50.0         |
      | 4  | FULLY_SETTLED     | 0.0          |
      | 5  | FULLY_SETTLED     | 0.0          |
    And the obligation state store should contain:
      | id | status        | openQuantity |
      | 4  | FULLY_SETTLED | 0.0          |
      | 5  | FULLY_SETTLED | 0.0          |

  Scenario: Amend open quantity when obligation is updated after receiving a confirmation
    Given the following obligations:
      | id | quantity |
      | 10 | 100      |
    When the following confirmations are received:
      | id   | obligationId | quantity |
      | blah | 10           | 20       |
    And the following obligations:
      | id | quantity |
      | 10 | 150      |
    Then the following obligation states are published:
      | id | openQuantity |
      | 10 | 100.0        |
      | 10 | 80.0         |
      | 10 | 130.0        |
    And the obligation state store should contain:
      | id | openQuantity |
      | 10 | 130.0        |
    