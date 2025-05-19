SELECT
    sa.store,
    sa.store_id,
    sa.name as app_name,
    sa.category as app_category,
    sa.installs,
    sa.rating_count,
    d.developer_id,
    d.name as developer_name,
    pd.url AS developer_domain_url,
    aae.publisher_id,
    aae.relationship,
    pd.crawled_at AS developer_domain_crawled_at
FROM frontend.adstxt_entries_store_apps aesa
LEFT JOIN store_apps sa 
    ON aesa.store_app = sa.id
LEFT JOIN developers d
    ON sa.developer = d.id
LEFT JOIN app_ads_entrys aae
    ON aesa.app_ad_entry_id = aae.id
LEFT JOIN pub_domains pd
    ON aesa.pub_domain_id = pd.id
LEFT JOIN ad_domains ad
    ON aesa.ad_domain_id = ad.id
WHERE
    ad."domain" = :ad_domain_url
    AND sa.store IS NOT NULL
;