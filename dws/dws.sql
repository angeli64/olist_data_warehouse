-- ============================================
-- 支付方式分析
-- 分析各支付方式的订单量、总金额、客单价及分期支付占比
-- 来源表：dwd_order_payments
-- ============================================


CREATE TABLE IF NOT EXISTS dws_payment_summary (
    payment_type      VARCHAR(20) COMMENT '支付方式',
    total_orders      INT COMMENT '订单数',
    total_amount      DECIMAL(15,2) COMMENT '总支付金额',
    avg_amount        DECIMAL(10,2) COMMENT '平均支付金额',
    installment_cnt   INT COMMENT '分期订单数',
    installment_rate  DECIMAL(5,2) COMMENT '分期占比%',
    etl_time          DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
)  COMMENT '支付方式汇总表';



INSERT INTO dws_payment_summary (
    payment_type,
    total_orders,
    total_amount,
    avg_amount,
    installment_cnt,
    installment_rate,
    etl_time
)
SELECT
    payment_type,
    COUNT(DISTINCT order_id),
    SUM(payment_value),
    AVG(payment_value),
    SUM(CASE WHEN is_installment = 1 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN is_installment = 1 THEN 1 ELSE 0 END) / COUNT(DISTINCT order_id) * 100, 2),
    NOW()
FROM dwd_order_payments
GROUP BY payment_type;



-- ============================================
-- 月度订单汇总分析
-- 分析各月份的订单量、完成取消情况、以及gmv
-- 来源表：dwd_order_payment、dwd_orders
-- ============================================

CREATE TABLE IF NOT EXISTS dws_order_monthly (
    order_month       VARCHAR(7) COMMENT '订单月份 yyyy-MM',
    total_orders      INT COMMENT '总订单数',
    completed_orders  INT COMMENT '完成订单数',
    canceled_orders   INT COMMENT '取消订单数',
    complete_rate     DECIMAL(5,2) COMMENT '完成率%',
    cancel_rate       DECIMAL(5,2) COMMENT '取消率%',
    monthly_gmv       DECIMAL(15,2) COMMENT '月度GMV',
    etl_time          DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间' 
)  COMMENT '月度订单汇总表';


INSERT INTO dws_order_monthly (
    order_month,
    total_orders,
    completed_orders,
    canceled_orders,
    complete_rate,
    cancel_rate,
    monthly_gmv,
    etl_time
)
SELECT
    o.order_month,
    o.total_orders,
    o.completed_orders,
    o.canceled_orders,
    ROUND(o.completed_orders / o.total_orders * 100, 2),
    ROUND(o.canceled_orders / o.total_orders * 100, 2),
    p.monthly_gmv,
    NOW()
FROM (
    SELECT
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m') as order_month,
        COUNT(DISTINCT order_id) as total_orders,
        SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) as completed_orders,
        SUM(CASE WHEN order_status = 'canceled' THEN 1 ELSE 0 END) as canceled_orders
    FROM dwd_orders
    GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m')
) o
JOIN (
    SELECT
        DATE_FORMAT(o2.order_purchase_timestamp, '%Y-%m') as order_month,
        SUM(p.payment_value) as monthly_gmv
    FROM dwd_orders o2
    JOIN dwd_order_payments p ON o2.order_id = p.order_id
    GROUP BY DATE_FORMAT(o2.order_purchase_timestamp, '%Y-%m')
) p ON o.order_month = p.order_month
ORDER BY o.order_month;



-- ============================================
-- 月度商品配送分析
-- 分析各月份的订单配送情况、平均配送时长以及超时率
-- 来源表：dwd_orders
-- ============================================

CREATE TABLE IF NOT EXISTS dws_delivery_summary (
    order_month         VARCHAR(7)  COMMENT '订单月份 yyyy-MM',
    total_delivered     INT COMMENT '完成配送订单数',
    avg_delivery_days   DECIMAL(5,2) COMMENT '平均配送天数',
    late_orders         INT COMMENT '超时订单数',
    late_rate           DECIMAL(5,2) COMMENT '超时率%',
    etl_time            DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '配送分析汇总表';




INSERT INTO dws_delivery_summary (
    order_month,
    total_delivered,
    avg_delivery_days,
    late_orders,
    late_rate,
    etl_time
)
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m'),
    SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END),
    AVG(deliver_days),
    SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN is_late = 1 THEN 1 ELSE 0 END) /  NULLIF(SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END), 0)*100,2),
    NOW()
FROM dwd_orders
GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m')
ORDER BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m');
