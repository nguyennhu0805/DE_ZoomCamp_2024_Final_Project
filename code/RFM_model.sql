create or replace table `theta-byte-412315.de_zoomcamp_final_project.RFM_ecommerce_electric` as
with 
raw_data AS 
(
SELECT distinct event_date, order_id, price, user_id
FROM `theta-byte-412315.de_zoomcamp_final_project.ecommerce_electric_data` 
WHERE user_id != 'nan'
)
, last_date AS
(
SELECT max (event_date) AS last_purchase_date
FROM raw_data
)
, RFM_calculate AS
(
SELECT user_id, date_diff (cast ('2020-12-21' as date), max (event_date), day) AS R, count (distinct order_id) AS F, sum (price) AS M
FROM raw_data
-- LEFT JOIN last_date on 1=1
GROUP BY 1
)
, RFM_percentile AS 
(
SELECT *,   
        PERCENT_RANK() OVER (ORDER BY R DESC) AS R_percentile,
        PERCENT_RANK() OVER (ORDER BY F ASC) AS F_percentile,
        PERCENT_RANK() OVER (ORDER BY F ASC) AS M_percentile
FROM RFM_calculate
)
, rfm_score AS(
    SELECT  *,
            CASE
                WHEN R_percentile >= 0.8 THEN '5'
                WHEN R_percentile >= 0.6 THEN '4'
                WHEN R_percentile >= 0.4 THEN '3'
                WHEN R_percentile >= 0.2 THEN '2'
                ELSE '1'
                END AS recency_score,
            CASE
                WHEN F_percentile >= 0.8 THEN '5'
                WHEN F_percentile >= 0.6 THEN '4'
                WHEN F_percentile >= 0.4 THEN '3'
                WHEN F_percentile >= 0.2 THEN '2'
                ELSE '1'
                END AS frequency_score,
            CASE
                WHEN M_percentile >= 0.8 THEN '5'
                WHEN M_percentile >= 0.6 THEN '4'
                WHEN M_percentile >= 0.4 THEN '3'
                WHEN M_percentile >= 0.2 THEN '2'
                ELSE '1'
                END AS monetary_score
    FROM rfm_percentile
)

SELECT *, concat (recency_score, frequency_score, monetary_score) AS rfm_cell,
        CASE
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('555', '554', '544', '545', '454', '455', '445') THEN 'Champion'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('543', '444', '435', '355', '354', '345', '344', '335') THEN 'Loyal'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('553', '551', '552', '541', '542', '533', '532', '531', '452', '451', '442', '441', '431', '453', '433', '432', '423', '353', '352', '351', '342', '341', '333', '323') THEN 'Potential Loyalist'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('512', '511', '422', '421', '412', '411', '311') THEN 'New Customers'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('525', '524', '523', '522', '521', '515', '514', '513', '425', '424', '413', '414', '415', '315', '314', '313') THEN 'Promising'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('535', '534', '443', '434', '343', '334', '325', '324') THEN 'Need Attention'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('331', '321', '312', '221', '213', '231', '241', '251') THEN 'About To Sleep'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('255', '254', '245', '244', '253', '252', '243', '242', '235', '234', '225', '224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124') THEN 'At Risk'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('155', '154', '144', '214', '215', '115', '114', '113') THEN 'Cannot Lose Them'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('332', '322', '233', '232', '223', '222', '132', '123', '122', '212', '211') THEN 'Hibernating Customers'
        WHEN concat (recency_score, frequency_score, monetary_score) IN ('111', '112', '121', '131', '141', '151') THEN 'Lost Customers'
        ELSE 'Other'
    END AS rfm_segment
FROM rfm_score
