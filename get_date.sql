With cte as
(SELECT 1 a
    UNION all
SELECT cte.a+1 a FROM cte)

SELECT TOP {a} cte.a FROM cte
