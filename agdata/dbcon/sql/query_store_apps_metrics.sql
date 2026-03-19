SELECT
    s.name AS store,
    sa.store_id AS store_id,
    sa.name AS app_name,
    sa.category AS app_category,
    d.developer_id,
    sa.price,
    sa.ad_supported,
    sa.in_app_purchases,
    agml.total_installs,
    agml.total_ratings AS total_ratings,
    agml.rating,
    sa.store_last_updated,
    sa.release_date,
    sa.updated_at AS appgoblin_updated_at,
    cr.outcome AS last_crawl_result 
FROM
    store_apps sa
LEFT JOIN stores s ON sa.store = s.id
LEFT JOIN developers d ON sa.developer = d.id 
LEFT JOIN app_global_metrics_latest agml ON sa.id = agml.store_app
LEFT JOIN crawl_results cr ON sa.crawl_result = cr.id
WHERE 
-- store app names are null when recently added and not yet crawled
sa.name IS NOT NULL
AND sa.crawl_result = 1
;
