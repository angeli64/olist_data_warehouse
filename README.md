# Olist 电商数据仓库项目

## 项目简介

基于[Olist 巴西电商数据集](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)，独立设计并实现完整的三层数据仓库架构（ODS / DWD / DWS），
涵盖数据建模、ETL开发、数据清洗及多主题聚合分析。

**技术栈：** MySQL

---

## 数据架构
```
CSV原始数据
 ↓
ODS层（贴源层）：原始数据导入，新增etl_time字段
 ↓
DWD层（明细层）：数据清洗、空值处理、新增派生字段
 ↓
DWS层（汇总层）：按主题聚合，输出分析指标

---

## 数据来源

Kaggle - Brazilian E-Commerce Public Dataset by Olist

本项目使用其中三张核心表：

| 表名 | 说明 | 数据量 |
|------|------|--------|
| olist_orders_dataset | 订单主表 | 99,441条 |
| olist_order_payments_dataset | 订单支付表 | 103,886条 |
| olist_order_items_dataset | 订单商品表 | 112,650条 |

---

## 分层设计

### ODS层

贴源存储，保留原始字段，新增`etl_time`记录导入时间。

### DWD层

在ODS层基础上进行数据清洗，新增派生字段：

| 表名 | 新增字段 | 说明 |
|------|----------|------|
| dwd_orders | deliver_days | 实际配送天数 |
| dwd_orders | is_late | 是否超时送达 |
| dwd_order_items | item_amount | 单件含运费金额 |
| dwd_order_payments | is_installment | 是否分期支付 |

### DWS层

按业务主题聚合，共三个主题：

| 表名 | 分析主题 | 核心指标 |
|------|----------|----------|
| dws_payment_summary | 支付方式分析 | 各支付方式订单量、GMV、客单价、分期占比 |
| dws_order_monthly | 月度订单分析 | 月度GMV、订单量、完成率、取消率 |
| dws_delivery_summary | 配送效率分析 | 平均配送天数、超时率 |

---


## 主要分析结论

**支付方式分析**
- 信用卡是主力支付方式，占总订单量约74%，其中67%选择分期付款
- Boleto（巴西银行划账）是第二大支付方式，不支持分期
- Voucher客单价最低，多用于优惠抵扣场景

**月度GMV趋势**
- 数据覆盖2016年9月至2018年10月，整体呈增长趋势
- 2018年上半年GMV达到峰值

**配送效率分析**
- 平均配送天数在5-12天之间
- 整体超时率较低，大部分月份在5%以内

---

## 数据质量说明

**坑1：支付表COUNT重复计算**
一个订单可能存在多条支付记录（组合支付），
统计订单数需使用 `COUNT(DISTINCT order_id)`，
使用 `COUNT(order_id)` 会导致重复计算。

**坑2：JOIN导致聚合结果失真**
orders表与payments表直接JOIN后，
CASE WHEN聚合会因行数放大导致完成率超过100%。
解决方案：两张表分别用子查询聚合后再JOIN。

**坑3：items与payments金额口径不一致**
order_items中的金额为商品原价+运费，
order_payments中的金额为实际支付金额，
两者存在差异，不能直接用于对账。
