SELECT
    sa.store,
    sa.store_id,
    sa.name as app_name,
    sa.category as app_category,
    sa.installs,
    sa.rating_count,
    d.developer_id,
    d.name as developer_name,
    aesa.developer_domain_url,
    aesa.publisher_id,
    aesa.relationship,
    aesa.developer_domain_crawled_at
FROM frontend.adstxt_entries_store_apps aesa
LEFT JOIN store_apps sa 
    ON aesa.store_app = sa.id
LEFT JOIN developers d
    ON sa.developer = d.id
WHERE
    aesa.ad_domain_url = :ad_domain_url
    AND sa.store IS NOT NULL
;