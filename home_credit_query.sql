WITH number_previous_credit AS(
    SELECT
        SK_ID_CURR,
        COUNT(SK_ID_BUREAU) AS previous_credit
    FROM bureau
    RIGHT JOIN application_train USING(SK_ID_CURR)
    GROUP BY SK_ID_CURR
) 
SELECT
    COUNT(DISTINCT SK_ID_CURR) As clients_with_previous_credit
FROM number_previous_credit
WHERE previous_credit > 0;

 clients_with_previous_credit 
------------------------------
                       263491

SELECT 
    COUNT(DISTINCT SK_ID_CURR)
FROM application_train;

SELECT 
    COUNT(DISTINCT SK_ID_CURR)
FROM bureau;

-- new table with join between bureau and application_train

WITH app_bureau AS(
    SELECT *
    FROM bureau
    RIGHT JOIN application_train USING(SK_ID_CURR)
)
SELECT
    COUNT(*)
FROM app_bureau;

-- correlation between target and:
-- number of days from past application and current application
-- number of days of overdue at the moment of current application
-- days to end past credit at the moment of current application

WITH app_bureau AS(
    SELECT *
    FROM bureau
    RIGHT JOIN application_train USING(SK_ID_CURR)
), imput_data AS(
    SELECT
        SK_ID_CURR,
        target, 
        COUNT(SK_ID_BUREAU) AS num_of_prev_bureau,
        AVG(days_credit) AS imp_days_credit,
        AVG(credit_day_overdue) AS imp_credit_day_overdue,
        AVG(days_credit_enddate) AS imp_days_credit_enddate,
        AVG(cnt_credit_prolong) AS imp_cnt_credit_prolong
    FROM app_bureau
    GROUP BY SK_ID_CURR, target
)
SELECT
    CORR(target, num_of_prev_bureau) AS corr_with_num_of_prev_bureau,
    CORR(target, imp_days_credit) AS corr_with_days_credit,
    CORR(target, imp_credit_day_overdue) AS corr_with_overdue_days,
    CORR(target, imp_days_credit_enddate) AS corr_with_days_to_end,
    CORR(target, imp_cnt_credit_prolong) AS corr_with_number_of_prolongations
FROM imput_data;

 corr_with_num_of_prev_bureau | 
------------------------------+
          -0.0100197156706841 |      

 corr_with_days_credit | corr_with_overdue_days | corr_with_days_to_end | corr_with_number_of_prolongations 
-----------------------+------------------------+-----------------------+-----------------------------------
     0.089728967220005 |    0.00811845370660204 |    0.0469827543348339 |               0.00303139567011085


WITH previous_bureau AS(
    SELECT
        SK_ID_CURR,
        COUNT(SK_ID_BUREAU) AS num_of_prev_bureau, 
        AVG(days_credit) AS avg_days_credit,
        AVG(credit_day_overdue) AS avg_credit_day_overdue,
        AVG(days_credit_enddate) AS avg_days_credit_enddate,
        AVG(cnt_credit_prolong) AS avg_cnt_credit_prolong
    FROM bureau
    GROUP BY SK_ID_CURR
)
SELECT
    COUNT(*)
FROM application_train
LEFT JOIN previous_bureau USING(SK_ID_CURR);


\copy (SELECT * FROM bureau RIGHT JOIN application_train USING(SK_ID_CURR)) TO '/Users/Chris/Desktop/home-credit-default-risk/app_bureau.csv' DELIMITER ',' CSV HEADER;


-- percentage of previuos credit with end date between 7 years before the current application
-- and 5 years after the current application. About 85%. The ohter 15% is weird data (like end date 120 years ago)

WITH app_bureau AS(
    SELECT *
    FROM bureau
    RIGHT JOIN application_train USING(SK_ID_CURR)
)
SELECT
    COUNT(CASE WHEN days_credit_enddate/365 BETWEEN -7 AND 5 THEN 1 END) / 
        (SELECT COUNT(*) FROM app_bureau)::numeric * 100 || '%' AS rows_ok
FROM app_bureau;

-- join bureau_balance table

WITH bureau_balance_imp AS(
    SELECT
        SK_ID_BUREAU,
        months_balance,
        CASE
        WHEN STATUS = 'C' THEN '0'
        WHEN STATUS = 'X' THEN '0'
        ELSE STATUS
        END AS new_status
    FROM bureau_balance
), bureau_status AS(
    SELECT
        SK_ID_BUREAU,
        SK_ID_CURR,
        MAX(new_status::numeric) AS max_bureau_status   
    FROM bureau_balance_imp
    JOIN bureau USING(SK_ID_BUREAU)
    GROUP BY SK_ID_BUREAU, SK_ID_CURR
), tot_bureau_status AS(
    SELECT
        SK_ID_CURR,
        AVG(max_bureau_status) AS avg_max_status
    FROM bureau_status
    GROUP BY SK_ID_CURR
)
SELECT
    CORR(target, avg_max_status) AS corr_with_avg_max_status
FROM tot_bureau_status
JOIN application_train USING(SK_ID_CURR);

 corr_with_avg_max_status 
--------------------------
       0.0430649307727489


SELECT COUNT(*) FROM bureau JOIN bureau_balance USING(SK_ID_BUREAU) WHERE STATUS <> 'C' AND STATUS <> 'X';

CREATE TEMP VIEW v1 AS


\copy (SELECT * FROM v1) TO '/Users/Chris/Desktop/home-credit-default-risk/tot_bureau.csv' DELIMITER ',' CSV HEADER;



SELECT COUNT(DISTINCT SK_ID_BUREAU) FROM bureau_balance WHERE STATUS = 'C';
 
 count  
--------
 449604

SELECT COUNT(DISTINCT SK_ID_BUREAU) FROM bureau_balance;
 
 count  
--------
 817395

SELECT COUNT(DISTINCT SK_ID_CURR) FROM bureau_balance join bureau using(SK_ID_BUREAU);

SELECT COUNT(DISTINCT SK_ID_BUREAU) FROM bureau;
  
  count  
---------
 1716428

 SELECT COUNT(DISTINCT SK_ID_CURR) FROM bureau join application_train using(SK_ID_CURR);

 SELECT credit_active, COUNT(credit_active) from bureau GROUP BY credit_active;

 Active        |  630607
 Bad debt      |      21
 Closed        | 1079273
 Sold          |    6527
 Tot           | 1716428

 SELECT COUNT(SK_ID_CURR) FROM application_train;

 count  
--------
 307511


WITH app_bureau AS(
    SELECT *
    FROM bureau
    RIGHT JOIN application_train USING(SK_ID_CURR)
)
SELECT COUNT(DISTINCT SK_ID_BUREAU) FROM app_bureau;
  
  count  
---------
 1465325

WITH app_with_doc_number AS(
    SELECT
        *,
        FLAG_DOCUMENT_2 + FLAG_DOCUMENT_3 + FLAG_DOCUMENT_3 + FLAG_DOCUMENT_4 + FLAG_DOCUMENT_5 +
            FLAG_DOCUMENT_6 + FLAG_DOCUMENT_7 + FLAG_DOCUMENT_8 + FLAG_DOCUMENT_9 + FLAG_DOCUMENT_10 +
                FLAG_DOCUMENT_11 + FLAG_DOCUMENT_12 + FLAG_DOCUMENT_13 + FLAG_DOCUMENT_14 + FLAG_DOCUMENT_15 +
                    FLAG_DOCUMENT_16 + FLAG_DOCUMENT_17 + FLAG_DOCUMENT_18 + FLAG_DOCUMENT_19 + FLAG_DOCUMENT_20 +
                        FLAG_DOCUMENT_21 AS number_of_doc
    FROM application_train
)
SELECT
    CORR(target, number_of_doc) AS corr_with_number_of_doc
FROM app_with_doc_number;

 corr_with_number_of_doc 
-------------------------
      0.0381686651224904


WITH org_type_num AS(
    SELECT
    DISTINCT organization_type,
    RANK() OVER(ORDER BY organization_type) AS org_type_number
FROM application_train
GROUP BY organization_type
)
SELECT
    CORR(target, org_type_number) AS corr_with_org_type
FROM application_train
JOIN org_type_num USING(organization_type);

 corr_with_org_type  
---------------------
 -0.0307653710422597


SELECT 
    COUNT(DISTINCT SK_ID_CURR) AS clients_with_previous_application
FROM application_train
JOIN previous_application USING(SK_ID_CURR);

 clients_with_previous_application 
-----------------------------------
                            291057

SELECT 
    COUNT(DISTINCT SK_ID_CURR) AS clients_with_previous_application
FROM application_train
JOIN pos_cash_balance USING(SK_ID_CURR);

 clients_with_previous_application 
-----------------------------------
                            289444

SELECT 
    COUNT(DISTINCT SK_ID_CURR) 
FROM application_train
LEFT JOIN pos_cash_balance USING(SK_ID_CURR)
LEFT JOIN bureau USING(SK_ID_CURR)
LEFT JOIN bureau_balance USING(SK_ID_BUREAU);


WITH previous_credit AS(
SELECT 
    SK_ID_PREV,
    SK_ID_CURR,
    MAX(cnt_instalment) AS num_of_inst,
    MIN(cnt_instalment_future) AS inst_left,
    MAX(SK_DPD_DEF) AS max_late
FROM pos_cash_balance
GROUP BY SK_ID_PREV, SK_ID_CURR
), previous_credit_info AS(
SELECT
    SK_ID_CURR,
    COUNT(DISTINCT SK_ID_PREV) AS num_of_previous_credit,
    AVG(num_of_inst) AS avg_inst,
    AVG(max_late) AS avg_max_late,
    SUM(inst_left) AS tot_inst_left
FROM previous_credit
GROUP BY SK_ID_CURR
)
SELECT
    CORR(target, num_of_previous_credit) AS corr_with_num_of_previous_credit,
    CORR(target, avg_inst) AS corr_with_avg_inst,
    CORR(target, avg_max_late) AS corr_with_avg_max_late,
    CORR(target, tot_inst_left) AS corr_with_tot_inst_left
FROM previous_credit_info
JOIN application_train USING(SK_ID_CURR);

 corr_with_num_of_previous_credit | corr_with_avg_inst | corr_with_avg_max_late | corr_with_tot_inst_left 
----------------------------------+--------------------+------------------------+-------------------------
              -0.0405074194951465 | 0.0301060676152377 |     0.0109060176122941 |     0.00958981531619368


-- GLOBAL DATASET

CREATE TEMP VIEW home_credit_data AS
WITH previous_credit AS(
SELECT 
    SK_ID_PREV,
    SK_ID_CURR,
    MAX(cnt_instalment) AS num_of_inst,
    MIN(cnt_instalment_future) AS inst_left,
    MAX(SK_DPD_DEF) AS max_late
FROM pos_cash_balance
GROUP BY SK_ID_PREV, SK_ID_CURR
), previous_credit_info AS(
SELECT
    SK_ID_CURR,
    COUNT(DISTINCT SK_ID_PREV) AS num_of_previous_credit,
    ROUND(AVG(num_of_inst)::numeric,1) AS avg_inst,
    ROUND(AVG(max_late::numeric),1) AS avg_max_late,
    SUM(inst_left) AS tot_inst_left
FROM previous_credit
GROUP BY SK_ID_CURR
), previous_bureau AS(
    SELECT
        SK_ID_CURR,
        COUNT(SK_ID_BUREAU) AS num_of_prev_bureau, 
        AVG(days_credit) AS avg_days_credit,
        AVG(credit_day_overdue) AS avg_credit_day_overdue,
        AVG(days_credit_enddate) AS avg_days_credit_enddate,
        AVG(cnt_credit_prolong) AS avg_cnt_credit_prolong
    FROM bureau
    GROUP BY SK_ID_CURR
), bureau_balance_imp AS(
    SELECT
        SK_ID_BUREAU,
        months_balance,
        CASE
        WHEN STATUS = 'C' THEN '0'
        WHEN STATUS = 'X' THEN '0'
        ELSE STATUS
        END AS new_status
    FROM bureau_balance
), bureau_status AS(
    SELECT
        SK_ID_BUREAU,
        SK_ID_CURR,
        MAX(new_status::numeric) AS max_bureau_status   
    FROM bureau_balance_imp
    JOIN bureau USING(SK_ID_BUREAU)
    GROUP BY SK_ID_BUREAU, SK_ID_CURR
), tot_bureau_status AS(
    SELECT
        SK_ID_CURR,
        AVG(max_bureau_status) AS avg_bureau_max_status
    FROM bureau_status
    GROUP BY SK_ID_CURR
)
SELECT
    *,
        FLAG_DOCUMENT_2 + FLAG_DOCUMENT_3 + FLAG_DOCUMENT_3 + FLAG_DOCUMENT_4 + FLAG_DOCUMENT_5 +
            FLAG_DOCUMENT_6 + FLAG_DOCUMENT_7 + FLAG_DOCUMENT_8 + FLAG_DOCUMENT_9 + FLAG_DOCUMENT_10 +
                FLAG_DOCUMENT_11 + FLAG_DOCUMENT_12 + FLAG_DOCUMENT_13 + FLAG_DOCUMENT_14 + FLAG_DOCUMENT_15 +
                    FLAG_DOCUMENT_16 + FLAG_DOCUMENT_17 + FLAG_DOCUMENT_18 + FLAG_DOCUMENT_19 + FLAG_DOCUMENT_20 +
                        FLAG_DOCUMENT_21 AS number_of_doc
FROM application_train 
LEFT JOIN previous_credit_info USING(SK_ID_CURR)
LEFT JOIN previous_bureau USING(SK_ID_CURR)
LEFT JOIN tot_bureau_status USING(SK_ID_CURR);


\copy (SELECT * FROM home_credit_data) TO '/Users/Chris/Desktop/home-credit-default-risk/home_credit_data.csv' DELIMITER ',' CSV HEADER;

SELECT
    COUNT(*)
FROM application_train
WHERE ABS(days_employed/365) > 50;

SELECT
    MAX(ABS(days_employed/365))
FROM application_train;

SELECT
    COUNT(*)
FROM application_train
WHERE (amt_credit/amt_annuity)/12 < 3;

SELECT
    COUNT(*)
FROM home_credit_data
WHERE num_of_prev_bureau BETWEEN 45 and 50;

SELECT
    COUNT(*)
FROM home_credit_data
WHERE avg_inst > 50;