select *
FROM
    {{ref('company_kpi')}}
where
    total_orders not between 1 and 50000