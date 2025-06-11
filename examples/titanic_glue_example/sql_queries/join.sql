select
    A.*, B.Survived
from
  <DESTINATION_GLUE_DATABASE>.materialized_test_table A
  left join
  <DESTINATION_GLUE_DATABASE>.materialized_submission_table B on
  A.PassengerId = B.PassengerId