-- Збір метрик виручки та активності користувачів у розрізі континентів.
-- Виводяться:
-- - Revenue (загальна виручка)
-- - Revenue from Mobile / Desktop
-- - % Revenue from Total
-- - Account Count / Verified Account Count / Session Count
-- Дані агрегуються з таблиць замовлень, продуктів, сесій та акаунтів.
-- Результат дозволяє оцінити комерційну ефективність і залученість користувачів на рівні континентів.


-- | Continent | Revenue | Revenue from Mobile | Revenue from Desktop |
--  % Revenue from Total | Account Count | Verified Account | Session Count |
 with revenu_up AS (
 SELECT
    sp.continent AS continent,
    SUM(p.price) AS revenue,
    sum(case when sp.device = 'mobile'then p.price end) as revenue_from_mobile,
    sum(case when sp.device != 'mobile'then p.price end) as revenue_from_desktop,
   FROM
     `DA.order` o
   JOIN
     `DA.product` p
   ON
     o.item_id = p.item_id
   JOIN
     `DA.session` s
   ON
     o.ga_session_id = s.ga_session_id
   Join  `DA.session_params` sp
   ON o.ga_session_id = sp.ga_session_id
GROUP BY sp.continent),


account_session_info AS(
SELECT
sp.continent,
count(ass.account_id) AS account_count,
count(CASE WHEN a.is_verified=1 then a.is_verified else NULL END) AS verified_account,
count(sp.ga_session_id) AS session_count
FROM `DA.session_params` sp
LEFT JOIN `DA.account_session` ass
ON sp.ga_session_id= ass.ga_session_id
LEFT JOIN `DA.account` a
ON ass.account_id= a.id
GROUP BY sp.continent
)


SELECT
r.continent,
revenue,
revenue_from_mobile,
revenue_from_desktop,
revenue/sum(revenue) over() * 100 AS  Revenue_from_Total,
account_count,
verified_account,
session_count
FROM revenu_up r
join account_session_info a
ON r.continent= a.continent
