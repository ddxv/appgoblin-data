SELECT
    s.name AS store,
    sa.store_id AS store_id,
    sa.name AS app_name,
    d.developer_id,
    d.name AS developer_name,
    sa.category AS app_category,
    sa.rating,
    sa.review_count,
    sa.rating_count,
    sa.installs,
    sa."free",
    sa.price,
    sa.minimum_android,
    sa.developer_email,
    sa.store_last_updated,
    sa.ad_supported,
    sa.in_app_purchases,
    sa.editors_choice,
    sa.release_date,
    sa.created_at AS appgoblin_created_at,
    sa.updated_at AS appgoblin_updated_at,
    cr.outcome AS last_crawl_result 
FROM
    store_apps sa
LEFT JOIN developers d ON sa.developer = d.id 
LEFT JOIN crawl_results cr ON sa.crawl_result = cr.id
LEFT JOIN stores s ON sa.store = s.id
WHERE 
-- store app names are null when recently added and not yet crawled
sa.name IS NOT NULL
;