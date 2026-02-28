CREATE DATABASE IF NOT EXISTS olist_dw;
USE olist_dw;

-- ODS层建表

-- 1. 订单表
CREATE TABLE IF NOT EXISTS ods_orders (
    order_id                         VARCHAR(50) NOT NULL COMMENT '订单ID',
    customer_id                      VARCHAR(50) COMMENT '客户ID',
    order_status                     VARCHAR(20) COMMENT '订单状态',
    order_purchase_timestamp         DATETIME COMMENT '下单时间',
    order_approved_at                DATETIME COMMENT '支付确认时间',
    order_delivered_carrier_date     DATETIME COMMENT '发货时间',
    order_delivered_customer_date    DATETIME COMMENT '实际送达时间',
    order_estimated_delivery_date    DATETIME COMMENT '预计送达时间',
    etl_time      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单原始表';



-- 2.订单支付表
CREATE TABLE IF NOT EXISTS ods_order_payments (
    order_id               VARCHAR(50) COMMENT '订单ID',
    payment_sequential     INT COMMENT '支付序号（同一订单多次支付）',
    payment_type           VARCHAR(30) COMMENT '支付方式',
    payment_installments   INT COMMENT '分期数',
    payment_value          DECIMAL(10,2) COMMENT '支付金额',
    etl_time   DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单支付原始表';



-- 3.订单商品表
CREATE TABLE IF NOT EXISTS ods_order_items (
    order_id               VARCHAR(50) COMMENT '订单ID',
    order_item_id          INT COMMENT '订单内商品序号',
    product_id             VARCHAR(50) COMMENT '商品ID',
    seller_id              VARCHAR(50) COMMENT '卖家ID',
    shipping_limit_date    DATETIME COMMENT '最晚发货时间',
    price                  DECIMAL(10,2) COMMENT '商品价格',
    freight_value          DECIMAL(10,2) COMMENT '运费',
    etl_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL导入时间'
) COMMENT '订单商品原始表';


-- 注意：以下路径需根据本地环境修改
-- 文件需放置在MySQL的secure_file_priv目录下

-- 导入订单表
LOAD DATA INFILE 'D:/MySQLData/Uploads/olist_orders_dataset.csv'
INTO TABLE ods_orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status, @ts1, @ts2, @ts3, @ts4, @ts5)
SET 
    order_purchase_timestamp = NULLIF(@ts1, ''),
    order_approved_at = NULLIF(@ts2, ''),
    order_delivered_carrier_date = NULLIF(@ts3, ''),
    order_delivered_customer_date = NULLIF(@ts4, ''),
    order_estimated_delivery_date = NULLIF(@ts5, ''),
    etl_time = NOW();


-- 导入订单支付表
LOAD DATA INFILE  'D:/MySQLData/Uploads/olist_order_payments_dataset.csv'
INTO TABLE ods_order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, payment_sequential, payment_type, payment_installments, payment_value)
SET
    etl_time = NOW();


-- 导入订单商品表
LOAD DATA INFILE 'D:/MySQLData/Uploads/olist_order_items_dataset.csv'
INTO TABLE ods_order_items
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, order_item_id, product_id, seller_id, @ts1, price, freight_value)
SET
    shipping_limit_date = NULLIF(@ts1, ''),
    etl_time = NOW();


