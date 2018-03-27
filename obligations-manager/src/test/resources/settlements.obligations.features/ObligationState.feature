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

  Scenario: Apply confirmation to obligation state
    Given the following new obligations:
      | id | security | quantity | amount |
      | 1  | first    | 200      | 100    |
      | 2  | second   | 20       | 10     |
    And the following obligation states are published:
      | id | openQuantity | openAmount | status |
      | 1  | 200.0        | 100.0      | OPEN   |
      | 2  | 20.0         | 10.0       | OPEN   |
    When the following confirmations are received:
      | id | obligationId | quantity |
      | A  | 1            | 150      |
      | B  | 1            | 50       |
      | C  | 2            | 20       |
    Then the following obligation states are published:
      | id | status            | openQuantity |
      | 1  | OPEN              | 200.0        |
      | 2  | OPEN              | 20.0         |
      | 1  | PARTIALLY_SETTLED | 50.0         |
      | 1  | FULLY_SETTLED     | 0.0          |
      | 2  | FULLY_SETTLED     | 0.0          |
    