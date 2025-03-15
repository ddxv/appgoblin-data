SELECT
    s.name AS store,
    sa.store_id AS store_id,
    sa.name AS app_name,
    sa.category AS app_category,
    sa.installs AS installs,
    sa.rating_count AS rating_count,
    sa.review_count AS review_count
FROM
    store_apps sa
LEFT JOIN stores s ON sa.store = s.id
WHERE 
-- store app names are null when recently added and not yet crawled
sa.name IS NOT NULL
AND sa.crawl_result = 1
;
