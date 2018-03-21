Feature: Obligation state lifecycle

  Scenario: New obligation creates a state event
    When The following new obligations:
      | id | security | quantity | amount | currency | direction |
      | 1  | abc      | 20       | 10     | USD      | DELIVER   |
    Then The following obligation states are published:
      | id | status | openQuantity | openAmount |
      | 1  | OPEN   | 20.0         | 10.0       |