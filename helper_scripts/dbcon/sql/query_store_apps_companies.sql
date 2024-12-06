SELECT
    sa.store_id,
    csac.ad_domain AS company_domain,
    c.name AS company_name,
    pc.name AS parent_company_name,
    csac.tag_source
FROM
    adtech.combined_store_apps_companies csac
LEFT JOIN store_apps sa ON
    csac.store_app = sa.id
LEFT JOIN adtech.companies c ON
    csac.company_id = c.id
LEFT JOIN adtech.companies pc ON
    csac.parent_id = pc.id
WHERE sa.crawl_result = 1
;
