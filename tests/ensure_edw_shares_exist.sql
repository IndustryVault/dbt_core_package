-- tests/assert_share_complete.sql
{{ validate_share(this.database) }}

-- macro raises if objects are missing; otherwise this returns 0 rows = pass
SELECT 1 AS missing_object WHERE 1 = 0
