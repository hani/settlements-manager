Feature: Obligation state lifecycle

  Scenario: New obligation creates a state event
    When the following new obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 20       | 10     |
    Then the following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |

  Scenario Outline: Apply confirmation to obligation state
    Given the following new obligations:
      | id | security | quantity | amount |
      | 1  | abc      | 200      | 100    |
      | 2  | abc      | 20       | 10     |
    When the following confirmations are received:
      | obligationId | quantity   |
      | <id>         | <quantity> |
    Then the following obligation states are published:
      | id   | status   | openQuantity   |
      | <id> | <status> | <openQuantity> |
    Examples:
      | id | quantity | status            | openQuantity |
      | 1  | 150      | PARTIALLY_SETTLED | 50           |
      | 1  | 50       | FULLY_SETTLED     | 0            |
      | 2  | 20       | FULLY_SETTLED     | 0            |
    