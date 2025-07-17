-- Створення трьох представлень (VIEW) для аналізу розподілу відправлених листів по місяцях.
-- Мета: обчислити частку листів кожного акаунта в загальному обсязі по місяцях, а також дати першої і останньої активності.
-- Вихід: sent_month, id_account, sent_msg_percent_from_this_month, first_sent_date, last_sent_date


CREATE VIEW `Students.v_t1_LES` AS (
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
);


CREATE VIEW `Students.v_t2_LES`AS(
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
GROUP BY id_account,sent_month
);


CREATE VIEW `Students.v_Aggregation_Data_with_View_LES`AS
SELECT
t1.sent_month AS sent_month,
t1.id_account AS id_account,
t1.sent_month_acc / t1.sent_month_all * 100 AS sent_msg_percen,
t2.first_sent_date AS first_sent_date,
t2.last_sent_date AS last_sent_date
FROM
`Students.v_t1_LES` t1 join `Students.v_t2_LES` t2
ON
t1.id_account= t2.id_account AND t1.sent_month =t2.sent_month;
