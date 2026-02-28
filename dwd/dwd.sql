-- ============================================
-- DWD层：订单清洗表
-- 在ODS基础上新增派生字段：deliver_days（配送天数）、is_late（是否超时）
-- 来源表：ods_orders
-- ============================================



CREATE TABLE IF NOT EXISTS dwd_orders (
    order_id                         VARCHAR(50) NOT NULL COMMENT '订单ID',
    customer_id                      VARCHAR(50) COMMENT '客户ID',
    order_status                     VARCHAR(20) COMMENT '订单状态',
    order_purchase_timestamp         DATETIME COMMENT '下单时间',
    order_approved_at                DATETIME COMMENT '支付确认时间',
    order_delivered_carrier_date     DATETIME COMMENT '发货时间',
    order_delivered_customer_date    DATETIME COMMENT '实际送达时间',
    order_estimated_delivery_date    DATETIME COMMENT '预计送达时间',
    deliver_days                     INT COMMENT '实际配送天数',
    is_late                          TINYINT(1) COMMENT '是否超时送达 1是0否',
    etl_time                         DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单清洗表';


-- 导入数据

INSERT INTO dwd_orders (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    deliver_days,
    is_late,
    etl_time
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    -- 计算实际配送天数
    DATEDIFF(order_delivered_customer_date,order_delivered_carrier_date) ,
    -- 判断是否超时
    CASE
        WHEN order_delivered_customer_date > order_estimated_delivery_date  THEN 1 
        ELSE 0
        END,
        NOW()
FROM ods_orders
WHERE order_id IS NOT NULL;




-- ============================================
-- DWD层：订单商品清洗表
-- 在ODS基础上新增派生字段：item_amount（单件含运费金额）
-- 来源表：ods_order_items
-- ============================================



CREATE TABLE IF NOT EXISTS dwd_order_items (
    order_id                VARCHAR(50) NOT NULL COMMENT '订单ID',
    order_item_id           INT COMMENT '订单内商品序号',
    product_id              VARCHAR(50) COMMENT '商品ID',
    seller_id               VARCHAR(50) COMMENT '卖家ID',
    shipping_limit_date     DATETIME COMMENT '最晚发货时间',
    price                   DECIMAL(10,2) COMMENT '商品价格',
    freight_value           DECIMAL(10,2) COMMENT '运费',
    item_amount             DECIMAL(10,2) COMMENT '单件含运费金额',
    etl_time                DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单商品清洗表';



INSERT INTO dwd_order_items (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    item_amount,
    etl_time
)
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    price + freight_value,
    NOW()
FROM ods_order_items
WHERE order_id IS NOT NULL;



-- ============================================
-- DWD层：订单支付清洗表
-- 在ODS基础上新增派生字段：is_installment（是否分期）
-- 来源表：ods_order_payments
-- ============================================







CREATE TABLE IF NOT EXISTS dwd_order_payments (
    order_id               VARCHAR(50) NOT NULL COMMENT '订单ID',
    payment_sequential     INT COMMENT '支付序号',
    payment_type           VARCHAR(20) COMMENT '支付方式',
    payment_installments   INT COMMENT '支付分期数',
    payment_value          DECIMAL(10,2) COMMENT '支付金额',
    is_installment         TINYINT(1) COMMENT '是否分期 1是0否',
    etl_time               DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单支付清洗表';




INSERT INTO dwd_order_payments (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    is_installment,
    etl_time
)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    CASE
        WHEN payment_installments > 1 THEN 1
        ELSE 0
        END,
    NOW()
FROM ods_order_payments
WHERE order_id IS NOT NULL;
