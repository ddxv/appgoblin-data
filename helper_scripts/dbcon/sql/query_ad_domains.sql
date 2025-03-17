WITH my_counts AS (
    SELECT
        company_domain,
        sum(app_count) AS app_count
    FROM
        frontend.companies_category_tag_stats cac
    WHERE
        cac.tag_source IN (
            'app_ads_direct', 'app_ads_reseller'
        )
    GROUP BY
        company_domain
)
SELECT
    company_domain,
    app_count
FROM
    my_counts
WHERE app_count > 10
ORDER BY
    app_count
;