Feature: Obligation state lifecycle

  Scenario: New obligation creates a state event
    When the following new obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 20       | 10     |
    Then the following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |
    And the obligation state store should contain:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |

  Scenario: Updated obligation overwrites existing one
    Given the following new obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 20       | 10     |
    When the following new obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 50       | 50     |
    Then the following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |
      | 1  | OPEN   | 50.0         | 50.0       |
    And the obligation state store should contain:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 50.0         | 50.0       |

  Scenario Outline: Apply confirmation to obligation state
    Given the following new obligations:
      | id | security | quantity | amount |
      | 1  | first    | 200      | 100    |
      | 2  | second   | 20       | 10     |
    And the following obligation states are published:
      | id | openQuantity | openAmount | status |
      | 1  | 200.0        | 100.0      | OPEN   |
      | 2  | 20.0         | 10.0       | OPEN   |
    When the following confirmations are received:
      | id   | obligationId | quantity   |
      | <id> | <id>         | <quantity> |
    Then the following obligation states are published:
      | id   | status   | openQuantity   |
      | <id> | <status> | <openQuantity> |
    Examples:
      | id | quantity | status            | openQuantity |
      | 1  | 150      | PARTIALLY_SETTLED | 50           |
      | 1  | 50       | FULLY_SETTLED     | 0            |
      | 2  | 20       | FULLY_SETTLED     | 0            |
    