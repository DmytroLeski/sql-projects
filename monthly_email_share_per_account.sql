-- SQL-запит для аналізу активності користувачів по місяцях.
-- Обчислюється відсоткова частка відправлених листів кожним акаунтом від загальної кількості в місяці.
-- Також визначаються дати першої та останньої активності (відправки email).
-- Виводяться: sent_month, id_account, sent_msg_percen, first_sent_date, last_sent_date.

WITH te AS (
  SELECT
    DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
    es.id_account,
    COUNT(es.id_message) OVER (PARTITION BY es.id_account, DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_month_acc,
    COUNT(es.id_message) OVER (PARTITION BY DATE_TRUNC(DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH)) AS sent_month_all
  FROM
    `data-analytics-mate.DA.email_sent` es
  JOIN
    `data-analytics-mate.DA.account_session` a ON es.id_account = a.account_id
  JOIN
    `data-analytics-mate.DA.session` s ON a.ga_session_id = s.ga_session_id
),
t1 AS (
SELECT
  te.id_account AS id_account,
  te.sent_month AS sent_month,
  te.sent_month_acc / te.sent_month_all * 100 AS sent_msg_percen
FROM te
),
t2 AS(
  SELECT
  id_account,
  DATE_TRUNC( DATE_ADD(s.date, INTERVAL es.sent_date DAY), MONTH) AS sent_month,
  MIN(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) AS first_sent_date,
  MAX(DATE_ADD(s.date, INTERVAL es.sent_date DAY)) AS last_sent_date
FROM
  `data-analytics-mate.DA.email_sent` es
JOIN `data-analytics-mate.DA.account_session` a
 ON es.id_account=a.account_id
 JOIN `data-analytics-mate.DA.session` s
 ON a.ga_session_id = s.ga_session_id  
GROUP BY id_account,sent_month)


SELECT  
DISTINCT(t1.sent_month),
t1.id_account AS id_account,
t1.sent_msg_percen AS sent_msg_percen,
t2.first_sent_date AS first_sent_date,
t2.last_sent_date AS last_sent_date
FROM
t1 join t2
ON
t1.id_account= t2.id_account AND t1.sent_month =t2.sent_month


