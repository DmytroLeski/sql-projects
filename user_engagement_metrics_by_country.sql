-- SQL-запит для збору метрик користувачів та email-активності
-- Мета: Аналіз залученості користувачів у розрізі країн, send_interval, статусу перевірки та підписки.
-- Обчислюються метрики: створені акаунти, відправлені/відкриті листи, переходи, а також ранжування країн.
-- Виводяться лише топ-10 країн за кількістю акаунтів або кількістю email-розсилок.
-- Для реалізації використано CTE, об'єднання через UNION та функції-вікна (DENSE_RANK).


WITH account AS (
    SELECT
        s.date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT(DISTINCT ac.id) AS account_cnt
    FROM `DA.account` ac
    JOIN `DA.account_session` acs ON ac.id = acs.account_id
    JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
    GROUP BY
        s.date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed
),


email_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) AS sent_date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` eo ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev ON es.id_message = ev.id_message
    JOIN `DA.account_session` acs ON es.id_account = acs.account_id
    JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
    JOIN `DA.account` ac ON es.id_account = ac.id
    GROUP BY
        DATE_ADD(s.date, INTERVAL es.sent_date DAY), sp.country,
        ac.send_interval, ac.is_verified, ac.is_unsubscribed
),


unions AS (
    SELECT
        date, country, send_interval, is_verified, is_unsubscribed,
        account_cnt, NULL AS sent_msg, NULL AS open_msg, NULL AS visit_msg
    FROM account
    UNION ALL
    SELECT
        sent_date, country, send_interval, is_verified, is_unsubscribed,
        NULL AS account_cnt, sent_msg, open_msg, visit_msg
    FROM email_metrics
),


final_groups AS (
    SELECT
        date, country, send_interval, is_verified, is_unsubscribed,
        SUM(COALESCE(account_cnt, 0)) AS account_cnt,
        SUM(COALESCE(sent_msg, 0)) AS sent_msg,
        SUM(COALESCE(open_msg, 0)) AS open_msg,
        SUM(COALESCE(visit_msg, 0)) AS visit_msg
    FROM unions
    GROUP BY
        date, country, send_interval, is_verified, is_unsubscribed
),


sums AS (
    SELECT
        country,
        SUM(account_cnt) AS total_country_account_cnt,
        SUM(sent_msg) AS total_country_sent_cnt,
        DENSE_RANK() OVER (ORDER BY SUM(account_cnt) DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY SUM(sent_msg) DESC) AS rank_total_country_sent_cnt
    FROM final_groups
    GROUP BY country
)


SELECT
    fg.date,
    fg.country,
    fg.send_interval,
    fg.is_verified,
    fg.is_unsubscribed,
    fg.account_cnt,
    fg.sent_msg,
    fg.open_msg,
    fg.visit_msg,
    s.total_country_account_cnt,
    s.total_country_sent_cnt,
    s.rank_total_country_account_cnt,
    s.rank_total_country_sent_cnt
FROM final_groups fg
JOIN sums s ON fg.country = s.country
WHERE s.rank_total_country_account_cnt <= 10 OR s.rank_total_country_sent_cnt <= 10
ORDER BY s.rank_total_country_account_cnt, s.rank_total_country_sent_cnt;
