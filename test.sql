WITH date_series AS (
    SELECT
         DATE_TRUNC('week', date)::DATE AS report_week
    FROM warehouse.dim_date
    WHERE date>=DATE('2020-09-02')
    AND date <= DATE_TRUNC('week', CURRENT_DATE)
   git  GROUP BY 1
    ORDER BY 1
), user_account_dates AS (
    SELECT
      user_id
    , DATE_TRUNC('week', activation_ts)::DATE                 AS activation_week
    , DATE_TRUNC('week', churned_date)::DATE                  AS churn_week
    FROM warehouse.dim_user
    WHERE activation_ts IS NOT NULL
    AND activation_ts >= '2020-09-02 15:00:00'
    AND tenant_id = 'pchealth'
    GROUP BY 1,2,3
), report_pch_kpi_weekly AS (
    SELECT
        r.user_id,
        date_trunc('week', date) as week,
        max(active_daily) as active_weekly
      FROM report_pchealth.report_pch_user_kpi r
      JOIN warehouse.dim_user du
        ON du.user_id = r.user_id
        AND du.activation_ts >= '2020-09-02 15:00:00'
        AND tenant_id = 'pchealth'
      GROUP BY 1,2
), engagement_weekly AS (
    SELECT
         user_id
       , week AS engagement_week
    FROM report_pch_kpi_weekly
    WHERE active_weekly=1
    AND week>=DATE('2020-08-31')
), cohort_base_table AS (
    SELECT
      uad.user_id
    , uad.activation_week
    , ds.report_week
    , uad.churn_week
    , ew.engagement_week
    , TRUNC(DATE_PART('day', report_week::timestamp - activation_week::timestamp)/7) as age
    , CASE WHEN churn_week=report_week - INTERVAL '1 MONTH' THEN 1
           WHEN churn_week<report_week THEN 1 ELSE 0 END AS churned
    , MAX(CASE WHEN engagement_week=report_week THEN 1 ELSE 0 END) AS engagement_action_taken
    FROM date_series ds
    JOIN user_account_dates uad
      ON uad.activation_week <= ds.report_week
    LEFT JOIN engagement_weekly ew
      ON uad.user_id=ew.user_id
     AND uad.activation_week<=ew.engagement_week
    WHERE report_week < DATE_TRUNC('week', CURRENT_DATE)
    GROUP BY 1,2,3,4,5,6,7
),

select_users as

(SELECT  distinct user_id

FROM cohort_base_table
where activation_week >= current_date - interval '90 days'
AND engagement_action_taken = 1
AND age =11
AND churned =0
)



SELECT
    su.user_id
   ,timestamp
   ,timestamp :: date as date
   ,category
   ,action
   ,label
FROM helper_ga_event hga
JOIN select_users su
on hga.user_Id =su.user_id