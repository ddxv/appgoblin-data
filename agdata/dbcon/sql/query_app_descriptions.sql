WITH latest_descriptions AS (
    SELECT DISTINCT ON (sad.store_app)
        sad.id AS description_id,
        sad.store_app,
        sad.description_short,
        sad.description,
        sad.updated_at AS description_last_updated
    FROM
        store_apps_descriptions AS sad
    WHERE
        sad.language_id = 1
    ORDER BY
        sad.store_app ASC,
        sad.updated_at DESC
    )
SELECT CASE WHEN sa.store = 1 THEN 'Android' ELSE 'iOS' END AS appstore, sa.store_id, sa.category, ld.description_short, ld.description, ld.description_last_updated from latest_descriptions ld
LEFT JOIN frontend.store_apps_overview sa ON ld.store_app = sa.id
;
