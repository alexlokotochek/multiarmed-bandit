metrics_sql_columns = [
    'model_version',
    'clicks',
    'phones',
    'shows',
    'users',
    'click_bounced',
    'phone_bounced'
]

metrics_sql = lambda last_events_cnt: """
-- вытаскиваем действия по каждой модели с номерами 
-- строк по убыванию даты (первый = самый последний)
WITH ranked_rows AS (
        SELECT 
                model_version,
                cid,
                clicked_cnt,
                phoned_cnt,
                recommended_dadd,
                rank() OVER (
                    PARTITION BY model_version 
                    ORDER BY recommended_dadd DESC) 
                AS event_order_from_last
        FROM pio_recs.sopr_shows 
        WHERE ptn_dadd >= current_date - interval '1' day
)

-- для каждой модели - самые свежие 
-- last_events_cnt действий и агрегируем
SELECT
        model_version,
        sum(clicked_cnt) clicks, 
        sum(phoned_cnt) phones, 
        count(*) shows, 
        count(distinct cid) users, 
        count(distinct case when clicked_cnt = 0 
                       then Null else cid end) click_bounced,
        count(distinct case when phoned_cnt = 0 
                       then Null else cid end) phone_bounced
FROM ranked_rows
WHERE event_order_from_last <= {0}
GROUP BY model_version
""".format(str(last_events_cnt))

