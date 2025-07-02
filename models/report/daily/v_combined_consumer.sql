   WITH f_orders_lines_combined AS (
    SELECT
        CUS_ORD_ID as TRANSACTION_ID,
        CUS_ORD_LN_ID as LINE_NO,
        IFNULL(F_CO_DSC_AMT_LCL, 0) + IFNULL(F_CO_UNIT_CST_LCL, 0) as ITEM_GROSS_PRICE,
        F_CO_ORD_QTY as ITEM_QTY,
        F_CO_TAX_AMT_LCL as ITEM_TAX,
        F_CO_UNIT_CST_LCL as ITEM_NET_PRICE,
        ITM_ID as PRODUCT_ID,
        F_CO_DSC_AMT_LCL as DISCOUNT_TOTAL,
        RCD_INS_TS,
        RCD_UPD_TS
    FROM
        ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_CUS_ORD_LN_B
    UNION
    SELECT
        *
    FROM
        ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_ORDER_LINES
),
f_orders_combined AS (
    WITH A as (
        SELECT
            CUS_ORD_ID,
            CUS_ID,
            TR_Billingfirstname,
            IFNULL(CUS_ID, TO_CHAR(RANDOM(1))) AS CUS_ID_SALTED,
            ORD_CREATED_DT,
            TR_ORDERTYPE,
            TR_TOTALNETPRICE,
            TR_ISYOTPOLOYALTYUSER,
            F_CO_TAX_AMT_LCL,
            RCD_INS_TS,
            RCD_UPD_TS
        FROM
            ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_CUS_ORD_HDR_B
    ),
    B AS (
        SELECT
            TRANSACTION_ID,
            CUSTOMER_ID,
            IFNULL(CUSTOMER_ID, TO_CHAR(RANDOM(2))) AS CUSTOMER_ID_SALTED,
            TRANSACTION_DATE,
            ORDER_CHANNEL,
            ORDER_NET_TOTAL,
            ORDER_TOTAL_TAX,
            RCD_INS_TS,
            RCD_UPD_TS
        FROM
            ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_ORDERS
    ),
    D AS (
        SELECT
            EMAIL,
            LOYALTY_TIER,
            JOIN_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY EMAIL
                ORDER BY
                    LOYALTY_RANK DESC
            ) AS ROW_NUMBER
        FROM
            (
                SELECT
                    Y.EMAIL,
                    Z.FIRST_TIER_DATE as JOIN_DATE,
                    CASE
                        WHEN RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Opening Act' THEN 'Insider'
                        WHEN RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Headliner' THEN 'Trendsetter'
                        ELSE RIGHT(Y.ACTION, LEN(Y.ACTION) - 12)
                    END as LOYALTY_TIER,
                    CASE
                        WHEN RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Opening Act'
                        OR RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Insider' THEN 1
                        WHEN RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Headliner'
                        OR RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Trendsetter' THEN 2
                        WHEN RIGHT(Y.ACTION, LEN(Y.ACTION) - 12) = 'Icon' THEN 3
                        ELSE 0
                    END as LOYALTY_RANK
                FROM
                    ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_YOTPO_HISTORY_ITEMS as Y
                    INNER JOIN (
                        SELECT
                            EMAIL,
                            MAX(CREATED_AT) as LATEST_TIER_DATE,
                            MIN(CREATED_AT) as FIRST_TIER_DATE
                        FROM
                            ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_YOTPO_HISTORY_ITEMS
                        WHERE
                            ACTION LIKE '%Earned Tier%'
                        GROUP BY
                            1
                    ) as Z ON Y.EMAIL = Z.EMAIL
                    AND Y.CREATED_AT = Z.LATEST_TIER_DATE
                WHERE
                    Y.ACTION LIKE '%Earned Tier%'
            )
    ),
    C AS (
        SELECT
            DISTINCT EMAIL_ADDRESS,
            FIRST_NAME,
            SFCC_CUSTOMER_ID,
            IFNULL(EMAIL_ADDRESS, TO_CHAR(RANDOM(3))) AS EMAIL_ADDRESS_SALTED,
            IFNULL(SFCC_CUSTOMER_ID, TO_CHAR(RANDOM(4))) AS SFCC_CUSTOMER_ID_SALTED,
            LOYALTY_TIER,
            --CAST(D_A.JOIN_DATE as DATE) AS LOYALTY_OPTIN_DT, --use this instead of line 86 once yotpo data is fixed, and update the 'group by' on 90
            CASE
                WHEN MAX(
                    CASE
                        WHEN IS_LOYALTY_MEMBER = 'true' THEN 1
                        ELSE 0
                    END
                ) = 1 THEN 'true'
                ELSE 'false'
            END as IS_LOYALTY_MEMBER,
            MAX(LOYALTY_OPTIN_STORE_NO) AS LOYALTY_OPTIN_STORE_NO,
            MAX(LOYALTY_OPTOUT_DT) AS LOYALTY_OPTOUT_DT,
            MIN(LOYALTY_OPTIN_DT) AS LOYALTY_OPTIN_DT --remove this once yotpo data is fixed to use first tier earn as the opt-in date
        FROM
           (Select * from ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_CUSTOMERS a 
        inner join  (select (Max(internal_id) OVER (Partition BY EMAIL_ADDRESS))as internal_id from ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_CUSTOMERS)  b on a.internal_id = b.internal_id
         
           group by all) as CUST
            LEFT JOIN (
                SELECT
                    EMAIL,
                    LOYALTY_TIER,
                    JOIN_DATE
                FROM
                    D
                WHERE
                    ROW_NUMBER = 1
            ) AS D_A ON CUST.EMAIL_ADDRESS = D_A.EMAIL
        GROUP BY
            all
    ),
    RT AS (
        SELECT
            DISTINCT TRIM(ORDER_IDS, '["]') AS ORDER_ID,
            '1' AS LOYALTY_REDEMPTION_FLAG
        FROM
            ROBLING_PRD_SFTP_LND_DB.DW_DWH_V.V_DWH_F_YOTPO_HISTORY_ITEMS
        WHERE
            ACTION LIKE '%Reward%'
    ),
    UPT AS (
        SELECT
            TRANSACTION_ID,
            SUM(ITEM_QTY) AS ORDER_ITEM_QTY
        FROM
            f_orders_lines_combined
        GROUP BY
            1
    ),
    COMBINED AS (
        SELECT
            A.CUS_ORD_ID as TRANSACTION_ID,
            --A.CUS_ID as SFCC_CUSTOMER_ID,
            C.EMAIL_ADDRESS as CUSTOMER_ID,
            A.ORD_CREATED_DT as TRANSACTION_DATE,
            A.TR_ORDERTYPE as ORDER_CHANNEL,
            CASE
                WHEN A.TR_ORDERTYPE = 'App' THEN 'E-Comm: App'
                WHEN A.TR_ORDERTYPE LIKE '%Instore%' THEN 'Retail'
                ELSE 'E-Comm: Site'
            END as ORDER_CHANNEL_AGG,
            A.TR_TOTALNETPRICE as ORDER_NET_TOTAL,
            A.F_CO_TAX_AMT_LCL as ORDER_TOTAL_TAX,
            C.IS_LOYALTY_MEMBER,
            --CASE WHEN C.IS_LOYALTY_MEMBER = 'true' AND CAST(C.LOYALTY_OPTIN_DT as DATE) <= CAST(A.ORD_CREATED_DT as DATE) THEN 'true'
            --  ELSE 'false' END as IS_LOYALTY_TRANSACTION,
            CASE
                WHEN LOWER(A.TR_ISYOTPOLOYALTYUSER) = 'true' THEN 'true'
                ELSE 'false'
            END as IS_LOYALTY_TRANSACTION,
            C.LOYALTY_OPTIN_STORE_NO,
            C.LOYALTY_OPTIN_DT,
            C.LOYALTY_OPTOUT_DT,
            CASE
                WHEN C.IS_LOYALTY_MEMBER = 'true' THEN C.LOYALTY_TIER
                ELSE NULL
            END as LOYALTY_TIER,
            A.RCD_INS_TS,
            A.RCD_UPD_TS
        FROM
            A
            LEFT JOIN C --ON A.CUS_ID_SALTED = C.SFCC_CUSTOMER_ID_SALTED
            ON (A.CUS_ID = C.SFCC_CUSTOMER_ID and a. TR_Billingfirstname = c.first_name)
        UNION
        SELECT
            B.TRANSACTION_ID,
            -- C.SFCC_CUSTOMER_ID as SFCC_CUSTOMER_ID,
            B.CUSTOMER_ID,
            B.TRANSACTION_DATE,
            B.ORDER_CHANNEL,
            'Retail' as ORDER_CHANNEL_AGG,
            B.ORDER_NET_TOTAL,
            B.ORDER_TOTAL_TAX,
            C.IS_LOYALTY_MEMBER,
            CASE
                WHEN CAST(C.LOYALTY_OPTIN_DT as DATE) <= CAST(B.TRANSACTION_DATE as DATE)
                AND COALESCE(
                    CAST(C.LOYALTY_OPTOUT_DT as DATE),
                    CURRENT_DATE()
                ) >= CAST(B.TRANSACTION_DATE as DATE) THEN 'true'
                ELSE 'false'
            END as IS_LOYALTY_TRANSACTION,
            C.LOYALTY_OPTIN_STORE_NO,
            C.LOYALTY_OPTIN_DT,
            C.LOYALTY_OPTOUT_DT,
            CASE
                WHEN C.IS_LOYALTY_MEMBER = 'true' THEN C.LOYALTY_TIER
                ELSE NULL
            END as LOYALTY_TIER,
            B.RCD_INS_TS,
            B.RCD_UPD_TS
        FROM
            B
            LEFT JOIN C --ON B.CUSTOMER_ID_SALTED = C.EMAIL_ADDRESS_SALTED
            ON B.CUSTOMER_ID = C.EMAIL_ADDRESS
    )
    SELECT
        COMBINED.TRANSACTION_ID,
        --SFCC_CUSTOMER_ID,
        CUSTOMER_ID,
        TO_DATE(TRANSACTION_DATE) TDATE,
        MIN(TRANSACTION_DATE) OVER (PARTITION BY CUSTOMER_ID) AS FIRST_TRANSACTION_DATE,
        MAX(TRANSACTION_DATE) OVER (PARTITION BY CUSTOMER_ID) AS LATEST_TRANSACTION_DATE,
        ORDER_CHANNEL,
        ORDER_CHANNEL_AGG,
        TRANSACTION_DATE,
        ORDER_NET_TOTAL,
        ORDER_TOTAL_TAX,
        ORDER_ITEM_QTY,
        IS_LOYALTY_MEMBER,
        IS_LOYALTY_TRANSACTION,
        LOYALTY_OPTIN_STORE_NO,
        LOYALTY_OPTIN_DT,
        LOYALTY_OPTOUT_DT,
        CASE
            WHEN IS_LOYALTY_TRANSACTION = 'true' THEN ORDER_NET_TOTAL
            ELSE NULL
        END AS LOYALTY_NET_TOTAL,
        CASE
            WHEN IS_LOYALTY_MEMBER IS NOT NULL
            AND LOYALTY_TIER IS NULL THEN 'Insider'
            ELSE LOYALTY_TIER
        END AS LOYALTY_TIER,
        LOYALTY_REDEMPTION_FLAG,
        CASE
            WHEN LOYALTY_REDEMPTION_FLAG = 1 THEN ORDER_NET_TOTAL
            ELSE NULL
        END AS REDEMPTION_NET_TOTAL,
        CASE
            WHEN LOYALTY_REDEMPTION_FLAG = 1 THEN ORDER_NET_TOTAL
            ELSE RT.ORDER_ID
        END AS REDEMPTION_TRANSACTION_ID,
        CASE
            WHEN ((
                MIN(TRANSACTION_DATE) OVER (PARTITION BY CUSTOMER_ID) = TRANSACTION_DATE
            ) AND CUSTOMER_ID IS NOT NULL) THEN 'NEW'
            WHEN ((
                MIN(TRANSACTION_DATE) OVER (PARTITION BY CUSTOMER_ID) < TRANSACTION_DATE
            ) AND CUSTOMER_ID IS NOT NULL) Then 'RETURNING'
            else 'UNKNOWN'
        END AS CUSTOMER_TYPE,
        RCD_INS_TS,
        RCD_UPD_TS
    FROM
        COMBINED
        LEFT JOIN RT ON COMBINED.TRANSACTION_ID = RT.ORDER_ID
        LEFT JOIN UPT ON COMBINED.TRANSACTION_ID = UPT.TRANSACTION_ID
), DIM_DATE AS (
  SELECT
    DATE AS DIM_DATE,
    DATE_LY,
    WEEK_ID,
    WEEK_NUMBER,
    QUARTER_ID,
    QUARTER_NAME
  FROM TR_PRD_ECOM_DB.ANALYSIS.DIM_DATE AS d
)
SELECT
  DIM_DATE,
  TDATE,
  DATE_LY,
  WEEK_ID,
  WEEK_NUMBER,
  QUARTER_ID,
  QUARTER_NAME,
  TRANSACTION_ID,
  CUSTOMER_ID,
  TRANSACTION_DATE,
  FIRST_TRANSACTION_DATE,
  LATEST_TRANSACTION_DATE,
  CUSTOMER_TYPE,
  ORDER_CHANNEL,
  ORDER_CHANNEL_AGG,
  sum(ORDER_NET_TOTAL) as ORDER_NET_TOTAL,
  Sum(ORDER_TOTAL_TAX) AS ORDER_TOTAL_TAX,
  sum(ORDER_ITEM_QTY) as ORDER_ITEM_QTY,
  IS_LOYALTY_MEMBER,
  IS_LOYALTY_TRANSACTION,
  LOYALTY_OPTIN_STORE_NO,
  LOYALTY_OPTIN_DT,
  LOYALTY_OPTOUT_DT,
  LOYALTY_NET_TOTAL,
  LOYALTY_TIER,
  LOYALTY_REDEMPTION_FLAG,
  REDEMPTION_NET_TOTAL,
  REDEMPTION_TRANSACTION_ID
FROM f_orders_combined
LEFT JOIN DIM_DATE
  ON DIM_DATE = TO_DATE(f_orders_combined.TRANSACTION_DATE)
  group by all