# <font color = 'green' >7月份下单用户行为分析</font>
<br>

## <font color = 'blue' >一、项目描述</font>

### **1. 背景描述**  
&emsp;&emsp;在当今激烈竞争的旅游市场中，深入了解用户行为并准确预测未来的订单成交趋势对于成功运营旅游平台至关重要。本项目致力于分析用户与某旅游平台进行互动、浏览或沟通的历史行为数据，以期准确预测他们未来的成交趋势。该项目的目标是优化市场营销策略，实现精细化营销，提高用户的成交转化率，从而推动平台的业绩增长。

### **2. 项目内容**
&emsp;&emsp;该项目针对7月份有下单行为的用户，选取该群体的用户侧、订单侧、流量侧、沟通侧的历史行为数据，分析该群体的基本信息特征，分析成交用户与未成交用户之间的行为差异，并通过机器学习方法来学习用户间差异，以预测用户未来的订单成交趋势。
&emsp;&emsp;**主要工作步骤如下：**  
1. **数据库取数：** 收集7月份下单用户数据，包括用户的个人特征、浏览历史、沟通历史以及交易历史等信息，周期为用户下单或成交前历史15天。
2. **数据分析：** 首先，进行了数据预处理，清理了异常值和缺失数据，以确保数据的质量和可用性。接着，采用了描述性和探索性数据分析的方法，挖掘了群体共性和群体间行为差异性。
3. **结论与建议：** 利用机器学习方法建立模型，探寻用户行为数据中隐性的关键模式和趋势，识别并预测用户未来一天的成交意向。

<br>



## <font color = 'blue' >二、导数</font>
### **1. 提取7月份成交订单和未成交订单的订单信息和用户信息** 
&emsp;&emsp;首先，基于订单表`dw.kn2_ord_order_detail_all`，提取7月份订单和下单用户的基本信息，结果保存在表`tmp_order_202307`。
``` sql
-- 选取模型1和模型4外呼的成交用户
drop table if exists tmp_call_transaction_cust;
create table tmp_call_transaction_cust as
select 
     cust_id
    ,label
    ,create_time
    ,vocation_mark
    ,travel_sign_amount
from dw.ol_dm_destination_predict_effect
where dt between '20230706' and '20230906' and type in (1, 4) 
    and order_mark_14=1 and travel_sign_amount is not null    -- 外呼后14天内成交
    and cust_id in (select distinct cust_id from dw.kn1_usr_cust_attribute);    -- 剔除已注销用户
```
&emsp;&emsp;创建临时表`tmp_order_cust_id`存储去重后的`cust_id`(用户ID)，降低后续运行工作量。
``` sql
-- 创建临时表存储去重后的cust_id，简化后续查询
drop table if exists tmp_order_cust_id;
create table tmp_order_cust_id as
select distinct cust_id from tmp_order_202307;
```
&emsp;&emsp;其次，基于表`dw.kn1_usr_cust_attribute`的会员身份信息，联合用户画像基本信息，进一步完善订购用户的详细信息，结果保存在表`tmp_order_cust_feature`。
``` sql
-- 订购用户画像
drop table if exists tmp_order_cust_feature;
create table tmp_order_cust_feature as
select
    a.cust_id,                       -- 会员ID
    a.cust_type,                     -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    a.cust_level,                    -- 会员星级
    a.is_inside_staff,               -- 是否内部员工:0否 1是
    a.is_distribution,               -- 是否分销:0否 1是
    a.is_scalper,                    -- 是否黄牛:0否 1是
    rs1.sex,                         -- 用户性别
    rs2.age,                         -- 用户年龄
    rs3.age_group,                   -- 用户年龄段
    rs4.tel_num,                     -- 用户手机号
    rs5.user_province,               -- 会员所在省
    rs6.user_city,                   -- 用户所在城市
    rs7.access_channel_cnt,          -- 可触达渠道的个数
    rs7.access_channels              -- 可触达渠道：push/企业微信/短信
from (
    select
        cust_id, cust_type, cust_level, is_inside_staff, is_distribution, is_scalper
    from dw.kn1_usr_cust_attribute
    where cust_id in (select * from tmp_order_cust_id)
) a
left join (
    --会员基本信息-性别
    select user_id, feature_name as sex
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '性别'
        and user_id in (select * from tmp_order_cust_id)
) rs1 on a.cust_id = rs1.user_id
left join (
    --会员基本信息-年龄
    select user_id, feature_value as age
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄'
        and user_id in (select * from tmp_order_cust_id)
) rs2 on a.cust_id = rs2.user_id
left join (
    --会员基本信息-年龄段
    select user_id, feature_name as age_group
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄段'
        and user_id in (select * from tmp_order_cust_id)
) rs3 on a.cust_id = rs3.user_id
left join (
    --会员基本信息-手机号
    select user_id, feature_value as tel_num
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '手机号'
        and user_id in (select * from tmp_order_cust_id)
) rs4 on a.cust_id = rs4.user_id
left join (
    --基本属性--基本信息-用户所在省
    select user_id, feature_name as user_province
    from dw.ol_rs_meta_feature_basic_info
    where three_class_name = '所在省'
        and user_id in (select * from tmp_order_cust_id)
) rs5 on a.cust_id = rs5.user_id
left join (
    --基本属性--基本信息-用户所在城市
    select user_id, feature_name as user_city
    from dw.ol_rs_meta_feature_basic_info
    where three_class_name = '所在城市'
        and user_id in (select * from tmp_order_cust_id)
) rs6 on a.cust_id = rs6.user_id
left join (
    --基本属性--个人信息-可触达渠道：push/企业微信/短信
    select 
        user_id, 
        count(1) as access_channel_cnt,
        concat_ws(',', collect_set(three_class_name)) as access_channels
    from dw.ol_rs_meta_feature_basic_info_access_channel
    where feature_value = 1 and user_id in (select * from tmp_order_cust_id)
    group by user_id
) rs7 on a.cust_id = rs7.user_id;
```


### **2. 基于7月份的订单信息，查询用户在下单前15天内的订单成交和投诉情况**
&emsp;&emsp;利用上述提取的7月份订单信息表`tmp_order_202307`，查询用户在下单或成交（订单日期order_date）前15天内的成交单数和投诉单数，结果保存在表`tmp_cust_historical_order`。
``` sql
-- 计算用户在下单前15内成交订单数、投诉订单数
drop table if exists tmp_cust_historical_order;
create table tmp_cust_historical_order as
select
    a.cust_id,
    a.order_id,
    a.executed_date,
    a.executed_flag,
    a.order_date,
    count(distinct case when b.executed_date between date_sub(a.order_date,15) and date_sub(a.order_date,1) then b.order_id else null end) executed_orders_count,   -- 历史15天内成交订单数
    count(distinct case when b.complaint_time between date_sub(a.order_date,15) and a.order_date then b.order_id else null end) complaint_orders_count              -- 历史15天内投诉订单数
from (
    select cust_id, order_id, executed_date, executed_flag, order_date
    from tmp_order_202307
) a
left join (
    select cust_id, order_id, executed_date, complaint_time
    from tmp_order_202307
) b
on a.cust_id = b.cust_id
group by a.cust_id, a.order_id, a.executed_date, a.executed_flag, a.order_date;
```


### **3. 提取用户在订单成交前的浏览行为相关信息**
&emsp;&emsp;首先，联合7月份订单信息表`tmp_order_202307`、流量域的app/pc/m流量表、产品表`dw.kn1_prd_route`和发券场景表`dw.ods_crmmkt_mkt_scene_clear_intention_cust_transform`，提取7月份下单用户在各平台的浏览时间、浏览产品信息、领取优惠券情况，结果保存在表`tmp_cust_browsing_info`。
``` sql
--  用户的浏览行为信息
drop table if exists tmp_cust_browsing_info;
create table tmp_cust_browsing_info as
select
    a.cust_id,                                              -- 会员编号
    a.order_id,                                             -- 订单号
    a.executed_date,                                        -- 订单成交日期
    a.executed_flag,                                        -- 订单成交标识
    a.order_date,                                           -- 订单日期
    b.operate_date,                                         -- 用户访问日期
    if(b.operate_date between date_sub(a.order_date, 15) and date_sub(a.order_date, 1), 1, 0) as browsing_flag,   -- 下单/成交前15天是否浏览 0：否，1：是
    b.product_id,                                           -- 产品ID
    b.residence_time,                                       -- 页面停留时间
    b.visitor_trace,                                        -- 访客标记
    c.lowest_price,                                         -- 浏览产品最低价
    d.producttype_class_name,                               -- 浏览产品大类
    nvl(e.collect_coupons_status, 0) collect_coupons_status -- 优惠券领取状态 1-成功 0-未领取/失败
from (
    select cust_id, order_id, executed_date, executed_flag, order_date
    from tmp_order_202307
) a
left join (
    -- 流量域
    select distinct vt_mapuid, to_date(operate_time) operate_date, product_id, residence_time, visitor_trace
    from dw.kn1_traf_app_day_detail
    where dt between '20230615' and '20230731'
        and vt_mapuid in (select * from tmp_order_cust_id) and vt_mapuid between 1 and 1000000001
    union
    select distinct vt_mapuid, to_date(operate_time) operate_date, product_id, residence_time, visitor_trace
    from dw.kn1_traf_day_detail
    where dt between '20230615' and '20230731'
        and vt_mapuid in (select * from tmp_order_cust_id) and vt_mapuid between 1 and 1000000001
) b on b.vt_mapuid = a.cust_id
left join (
    -- 产品最低价
    select distinct route_id, lowest_price
    from dw.kn1_prd_route
) c on c.route_id = b.product_id
left join (
    select 
        distinct route_id,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) d on d.route_id = b.product_id
left join (
    -- 用户领券情况
    select distinct cust_id, collect_coupons_status, to_date(operate_time) operate_date
    from dw.ods_crmmkt_mkt_scene_clear_intention_cust_transform
    where to_date(operate_time) between '2023-06-15' and '2023-07-31'
        and collect_coupons_status = 1 and cust_id in (select * from tmp_order_cust_id)
) e on e.cust_id = b.vt_mapuid and e.operate_date = b.operate_date;
```
&emsp;&emsp;其次，在表`tmp_cust_browsing_info`的基础上，计算用户历史15天内的浏览行为相关指标值，包括初始浏览时间、浏览天数、PV、浏览时间、浏览产品量、浏览产品价格、度假产品浏览量、单资源产品浏览量、优惠券领取个数，结果保存在表`tmp_cust_communication_behavior_info`。
``` sql
-- 用户的浏览行为信息--计算具体指标
drop table if exists tmp_cust_browsing_behavior_info;
create table tmp_cust_browsing_behavior_info as
select
    a.cust_id,                                                                  -- 会员编号
    a.order_id,                                                                 -- 订单号
    a.executed_date,                                                            -- 订单成交日期
    a.executed_flag,                                                            -- 订单成交标识
    a.order_date,                                                               -- 订单日期
    a.browsing_flag,                                                            -- 下单/成交前15天是否浏览 0：否，1：是
    b.first_operate_date,                                                       -- 历史15天内第一次浏览时间
    nvl(b.browsing_days, 0) browsing_days,                                      -- 历史浏览天数 
    nvl(b.pv, 0) pv,                                                            -- 用户总pv
    nvl(b.pv_daily_avg, 0) pv_daily_avg,                                        -- 每日平均pv
    nvl(b.browsing_time, 0) as browsing_time,                                   -- 用户总浏览时间
    nvl(b.browsing_time_daily_avg, 0) browsing_time_daily_avg,                  -- 每日平均浏览时间
    nvl(b.browsing_products_cnt, 0) browsing_products_cnt,                      -- 历史浏览产品量
    nvl(b.browsing_products_cnt_daily_avg, 0) browsing_products_cnt_daily_avg,  -- 每日平均浏览产品量
    b.lowest_price_avg,                                                         -- 历史浏览产品（最低价）平均价
    nvl(b.browsing_vac_prd_cnt, 0) browsing_vac_prd_cnt,                        -- 历史浏览的度假产品数量
    nvl(b.browsing_single_prd_cnt, 0) browsing_single_prd_cnt,                  -- 历史浏览的单资源产品数量
    nvl(b.collect_coupons_cnt, 0) collect_coupons_cnt                           -- 优惠券领券个数
from (
    select 
        cust_id, order_id, executed_date, executed_flag, order_date, 
        case when max(browsing_flag) = 1 then 1 else 0 end browsing_flag        -- 过滤：同一cust_id、order_id的browsing_flag可同时取0和1，对此只保留browsing_flag=1的样本
    from tmp_cust_browsing_info
    group by cust_id, order_id, executed_date, executed_flag, order_date
) a
left join (
    select  
        cust_id, order_id, browsing_flag,
        min(operate_date) first_operate_date,
        count(distinct operate_date) browsing_days,
        count(visitor_trace) pv,
        round(count(visitor_trace) / count(distinct operate_date), 4) pv_daily_avg,
        sum(residence_time) browsing_time,
        round(sum(residence_time) / count(distinct operate_date), 4) browsing_time_daily_avg,
        count(distinct product_id) browsing_products_cnt,
        round(count(distinct product_id) / count(distinct operate_date), 4) browsing_products_cnt_daily_avg,
        round(avg(lowest_price), 4) lowest_price_avg,
        sum(case when producttype_class_name like '度假产品' then 1 else 0 end) browsing_vac_prd_cnt,
        sum(case when producttype_class_name like '单资源产品' then 1 else 0 end) browsing_single_prd_cnt,
        count(distinct case when collect_coupons_status = 1 then operate_date else null end) collect_coupons_cnt
    from tmp_cust_browsing_info
    where browsing_flag = 1         -- 只对有浏览行为的用户计算指标
    group by cust_id, order_id, browsing_flag
) b on a.cust_id = b.cust_id and a.order_id = b.order_id and a.browsing_flag = b.browsing_flag;
```


### **4. 基于订单域、电话明细、数据各渠道触客明细，提取用户在订单成交前各渠道的沟通行为相关信息**
&emsp;&emsp;根据6月份用户沟通渠道信息的分析结果，发现电话、智能外呼、企微聊天、在线客服这4个渠道与用户的接触较多，故提取7月份这4个渠道与用户的沟通行为数据，包括沟通日期、沟通渠道、沟通持续时长、沟通量、用户主动沟通量、度假产品提及次数、单资源产品提及次数，结果保存在表`tmp_cust_communication_info`。
``` sql
-- 用户的沟通行为信息
drop table if exists tmp_cust_communication_info;
create table tmp_cust_communication_info as
select
    a.order_id,                             -- 订单号
    a.cust_id,                              -- 会员编号
    a.executed_date,                        -- 订单成交日期
    a.executed_flag,                        -- 订单成交标识
    a.order_date,                           -- 订单日期
    b.create_time,                          -- 沟通日期
    if(b.create_time between date_sub(a.order_date, 15) and date_sub(a.order_date, 1), 1, 0) as comm_flag,   -- 下单/成交前15天是否沟通 0：否，1：是
    b.channel,                              -- 沟通渠道名称
    b.channel_id,                           -- 沟通渠道ID 1-电话呼入呼出 2-智能外呼 3-企微聊天 4-在线客服
    b.comm_duration,                        -- 沟通持续时长
    b.comm_num,                             -- 沟通量：通话量/聊天数
    b.active_comm_num,                      -- 用户主动进行沟通的数量
    b.vac_mention_num,                      -- 沟通过程中度假产品的提及次数
    b.single_mention_num                    -- 沟通过程中单资源产品的提及次数
from (
    select cust_id, order_id, executed_date, executed_flag, order_date
    from tmp_order_202307
) a
left join (
    -- 电话明细
    select distinct cust_id, create_time, comm_duration, comm_num, active_comm_num, vac_mention_num, single_mention_num, '电话呼入呼出' as channel, 1 as channel_id
    from (
        select
            cust_id, create_time,
            sum(status_time) comm_duration,     -- 通话总时长
            count(1) comm_num,                  -- 接通量
            sum(case when calldir='呼入' then 1 else 0 end) active_comm_num,   -- 用户呼入量
            sum(case when producttype_class_name like '度假产品' then 1 else 0 end) vac_mention_num,
            sum(case when producttype_class_name like '单资源产品' then 1 else 0 end) single_mention_num
        from (
            select cust_id, tel_num
            from tmp_order_cust_feature
        ) t1
        left join (
            select
                case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num,
                to_date(status_start_time) create_time,
                status_time, calldir, order_id
            from dw.kn1_call_tel_order_detail
            where dt between '20230615' and '20230731' and status='通话'    -- 统计接通用户数据        
        ) t2 on t2.tel_num = t1.tel_num
        left join (
            select order_id, route_id, book_city from dw.kn2_ord_order_detail_all
            where dt = '20230731' and create_time >= '2023-06-01'
        ) t3 on t3.order_id = t2.order_id
        left join (
            select distinct route_id, book_city,
                case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
            from dw.kn2_dim_route_product_sale
        ) t4 on t4.route_id = t3.route_id and t4.book_city = t3.book_city
        where cust_id in (select * from tmp_order_cust_id)
        group by cust_id, create_time
    ) t
    union all
    -- 机器人外呼明细
    select
        cust_id, to_date(answer_time) create_time,
        sum(call_time) comm_duration,           -- 通话总时长
        count(1) comm_num,                      -- 接通量
        sum(case when generate_task_is = 1 and label <> 'D' then 1 else 0 end) active_comm_num,   -- 命中用户意向量
        0 as vac_mention_num, 0 as single_mention_num,
        '智能外呼' as channel, 2 as channel_id
    from dw.kn1_sms_robot_outbound_call_detail
    where dt between '20230615' and '20230731' and answer_flag = 1      -- 统计接通用户数据
        and cust_id in (select * from tmp_order_cust_id)
    group by cust_id, to_date(answer_time)
    union all
    -- 企微聊天明细
    select 
        cust_id, to_date(msg_time) create_time,
        null as comm_duration,                  -- 聊天时长：null
        count(msg_time) comm_num,               -- 发送消息数
        sum(case when type=1 then 1 else 0 end) active_comm_num,   -- 用户主动聊天数
        sum(case when contact like '%商旅%' or contact like '%跟团%' or contact like '%自驾%' or contact like '%自助%'
                or contact like '%目的地服务%' or contact like '%签证%' or contact like '%团队%' or contact like '%定制%'
                or contact like '%游轮%' or contact like '%旅拍%' or contact like '%游%' or contact like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when contact like '%火车票%' or contact like '%机票%' or contact like '%酒店%' or contact like '%百货%'
                or contact like '%用车服务%' or contact like '%高铁%' or contact like '%票%' or contact like '%硬座%'
                or contact like '%软卧%' or contact like '%卧铺%' or contact like '%航班%' then 1 else 0 end) single_mention_num,
        '企微聊天' as channel, 3 as channel_id
    from dw.kn1_officeweixin_sender_cust_content
    where dt between '20230615' and '20230731'
        and cust_id in (select * from tmp_order_cust_id)
    group by cust_id, to_date(msg_time)
    union all
    -- 在线客服沟通明细
    select
        cust_id,
        to_date(create_start_time) create_time,
        null as comm_duration,                  -- 聊天时长：null
        count(1) comm_num,                      -- 发送消息数
        sum(case when content like '%客人发送消息%' then 1 else 0 end) active_comm_num,    -- 用户主动发送消息数
        sum(case when content like '%商旅%' or content like '%跟团%' or content like '%自驾%' or content like '%自助%'
                or content like '%目的地服务%' or content like '%签证%' or content like '%团队%' or content like '%定制%'
                or content like '%游轮%' or content like '%旅拍%' or content like '%游%' or content like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when content like '%火车票%' or content like '%机票%' or content like '%酒店%' or content like '%百货%'
                or content like '%用车服务%' or content like '%高铁%' or content like '%票%' or content like '%硬座%'
                or content like '%软卧%' or content like '%卧铺%' or content like '%航班%' then 1 else 0 end) single_mention_num,
        '在线客服' as channel, 4 as channel_id
    from dw.kn1_autotask_user_acsessed_chat
    where dt between '20230615' and '20230731'
        and cust_id in (select * from tmp_order_cust_id)
    group by cust_id, to_date(create_start_time)
) b on b.cust_id = a.cust_id;
```
&emsp;&emsp;其次，在表`tmp_cust_communication_info`的基础上，计算用户历史15天内的沟通行为相关指标值，包括初次沟通时间、沟通天数、沟通次数、各渠道沟通数占比、沟通渠道数、沟通时长、通话频数、用户主动通话频数及占比、聊天频数、用户主动聊天频数及占比、度假产品总提及次数、单资源产品总提及次数等，结果保存在表`tmp_cust_communication_behavior_info`。
``` sql
-- 用户的沟通行为信息--计算具体指标
drop table if exists tmp_cust_communication_behavior_info;
create table tmp_cust_communication_behavior_info as
select 
    a.cust_id,                                              -- 会员编号
    a.order_id,                                             -- 订单号
    a.executed_date,                                        -- 订单成交日期
    a.executed_flag,                                        -- 订单成交标识
    a.order_date,                                           -- 订单日期
    a.comm_flag,                                            -- 下单/成交前15天是否沟通 0：否，1：是
    b.first_create_time,                                    -- 历史15天内第一次沟通时间
    nvl(b.comm_days, 0) comm_days,                          -- 历史沟通天数
    nvl(b.comm_freq, 0) comm_freq,                          -- 总沟通次数（count(channel)）
    nvl(b.comm_freq_daily_avg, 0) comm_freq_daily_avg,      -- 每日平均沟通次数
    nvl(b.call_pct, 0) call_pct,                            -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    nvl(b.robot_pct, 0) robot_pct,                          -- 智能外呼占比
    nvl(b.officewx_pct, 0) officewx_pct,                    -- 企微聊天占比
    nvl(b.chat_pct, 0) chat_pct,                            -- 在线客服占比
    nvl(b.channels_cnt, 0) as channels_cnt,                 -- 历史沟通渠道数（count(distinct channel)）
    nvl(b.comm_time, 0) comm_time,                          -- 总沟通时长（特指电话、智能外呼）
    nvl(b.comm_time_daily_avg, 0) comm_time_daily_avg,      -- 每日平均沟通时长
    nvl(b.calls_freq, 0) calls_freq,                        -- 通话频数：电话+智能外呼
    nvl(b.calls_freq_daily_avg, 0) calls_freq_daily_avg,    -- 每日平均通话频数：电话+智能外呼
    nvl(b.active_calls_freq, 0) active_calls_freq,          -- 用户主动通话频数：电话+智能外呼
    nvl(b.active_calls_pct, 0) active_calls_pct,            -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    nvl(b.chats_freq, 0) chats_freq,                        -- 聊天频数：企微聊天+在线客服
    nvl(b.chats_freq_daily_avg, 0) chats_freq_daily_avg,    -- 每日平均聊天频数：企微聊天+在线客服
    nvl(b.active_chats_freq, 0) active_chats_freq,          -- 用户主动聊天频数：企微聊天+在线客服
    nvl(b.active_chats_pct, 0) active_chats_pct,            -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    nvl(b.vac_mention_num, 0) vac_mention_num,              -- 沟通过程中度假产品的总提及次数
    nvl(b.single_mention_num, 0) single_mention_num         -- 沟通过程中度假产品的总提及次数
from (
    select 
        cust_id, order_id, executed_date, executed_flag, order_date, 
        case when max(comm_flag) = 1 then 1 else 0 end comm_flag        -- 过滤：同一cust_id、order_id的comm_flag可同时取0和1，对此只保留comm_flag=1的样本
    from tmp_cust_communication_info
    group by cust_id, order_id, executed_date, executed_flag, order_date
) a
left join (
    select  
        cust_id, order_id, comm_flag,
        min(create_time) first_create_time,
        count(distinct create_time) comm_days,
        count(channel_id) comm_freq,
        round(count(channel_id)/count(distinct create_time), 4) comm_freq_daily_avg,
        round(sum(if(channel_id=1,1,0)) / count(channel_id), 4) call_pct,
        round(sum(if(channel_id=2,1,0)) / count(channel_id), 4) robot_pct,
        round(sum(if(channel_id=3,1,0)) / count(channel_id), 4) officewx_pct,
        round(sum(if(channel_id=4,1,0)) / count(channel_id), 4) chat_pct,
        count(distinct channel_id) channels_cnt,
        sum(comm_duration) comm_time,
        round(sum(comm_duration)/count(distinct create_time), 4) comm_time_daily_avg,
        sum(if(channel_id in (1,2), comm_num, 0)) calls_freq,
        round(sum(if(channel_id in (1,2), comm_num, 0)) / count(distinct if(channel_id in (1,2), create_time, null)), 4) calls_freq_daily_avg,
        sum(if(channel_id in (1,2), active_comm_num, 0)) active_calls_freq,
        round(sum(if(channel_id in (1,2), active_comm_num, 0))/sum(if(channel_id in (1,2), comm_num, 0)), 4) active_calls_pct,
        sum(if(channel_id in (3,4), comm_num, 0)) chats_freq,
        round(sum(if(channel_id in (3,4), comm_num, 0)) / count(distinct if(channel_id in (3,4), create_time, null)), 4) chats_freq_daily_avg,
        sum(if(channel_id in (3,4), active_comm_num, 0)) active_chats_freq,
        round(sum(if(channel_id in (3,4), active_comm_num, 0))/sum(if(channel_id in (3,4), comm_num, 0)), 4) active_chats_pct,
        sum(vac_mention_num) vac_mention_num,
        sum(single_mention_num) single_mention_num
    from tmp_cust_communication_info
    where comm_flag = 1         -- 只对有沟通行为的用户计算指标
    group by cust_id, order_id, comm_flag
) b on a.cust_id = b.cust_id and a.order_id = b.order_id and a.comm_flag = b.comm_flag;
```


### **5. 数据导出及数据表基本信息汇总**
&emsp;&emsp;汇总以上临时表，存储7月份订单信息、下单用户信息以及用户在下单前15天内的订单成交和投诉信息、沟通和浏览行为信息，剔除内部会员、分销、黄牛的订单数据，结果保存在表`tmp_order_cust_feature_behavior_info_202307`。
``` sql
-- 汇总以上表
drop table if exists tmp_order_cust_feature_behavior_info_202307;
create table tmp_order_cust_feature_behavior_info_202307 as
select
    t1.*,
    t2.cust_type,                       -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    t2.sex,                             -- 用户性别
    t2.age_group,                       -- 用户年龄段
    t2.user_province,                   -- 会员所在省
    t2.user_city,                       -- 用户所在城市
    t2.access_channel_cnt,              -- 可触达渠道的个数
    t3.executed_orders_count,           -- 历史15天内成交订单数
    t3.complaint_orders_count,          -- 历史15天内投诉订单数
    nvl(datediff(t1.order_date, least(t4.first_operate_date, t5.first_create_time)), 0) decision_time,    -- 决策时间
    t4.browsing_flag,                   -- 下单/成交前15天是否浏览 0：否，1：是
    t4.first_operate_date,              -- 历史15天内一次浏览时间
    t4.browsing_days,                   -- 历史浏览天数 
    t4.pv,                              -- 用户总pv
    t4.pv_daily_avg,                    -- 每日平均pv
    t4.browsing_time,                   -- 用户总浏览时间
    t4.browsing_time_daily_avg,         -- 每日平均浏览时间
    t4.browsing_products_cnt,           -- 历史浏览产品量
    t4.browsing_products_cnt_daily_avg, -- 每日平均浏览产品量
    t4.lowest_price_avg,                -- 历史浏览产品（最低价）平均价
    t4.browsing_vac_prd_cnt,            -- 历史浏览的度假产品数量
    t4.browsing_single_prd_cnt,         -- 历史浏览的单资源产品数量
    t4.collect_coupons_cnt,             -- 优惠券领券个数
    t5.comm_flag,                       -- 下单/成交前15天是否沟通 0：否，1：是
    t5.first_create_time,               -- 历史15天内一次沟通时间
    t5.comm_days,                       -- 历史沟通天数
    t5.comm_freq,                       -- 总沟通次数（count(channel)）
    t5.comm_freq_daily_avg,             -- 每日平均沟通次数
    t5.call_pct,                        -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    t5.robot_pct,                       -- 智能外呼占比
    t5.officewx_pct,                    -- 企微聊天占比
    t5.chat_pct,                        -- 在线客服占比
    t5.channels_cnt,                    -- 历史沟通渠道数（count(distinct channel)）
    t5.comm_time,                       -- 总沟通时长（特指电话、智能外呼）
    t5.comm_time_daily_avg,             -- 每日平均沟通时长
    t5.calls_freq,                      -- 通话频数：电话+智能外呼
    t5.calls_freq_daily_avg,            -- 每日平均通话频数：电话+智能外呼
    t5.active_calls_freq,               -- 用户主动通话频数：电话+智能外呼
    t5.active_calls_pct,                -- 用户主动通话占比 = 用户主动通话频数 / 通话频数
    t5.chats_freq,                      -- 聊天频数：企微聊天+在线客服
    t5.chats_freq_daily_avg,            -- 每日平均聊天频数：企微聊天+在线客服
    t5.active_chats_freq,               -- 用户主动聊天频数：企微聊天+在线客服
    t5.active_chats_pct,                -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    t5.vac_mention_num,                 -- 沟通过程中度假产品的总提及次数
    t5.single_mention_num               -- 沟通过程中度假产品的总提及次数
from tmp_order_202307 t1
left join tmp_order_cust_feature t2
on t2.cust_id = t1.cust_id
left join tmp_cust_historical_order t3
on t3.cust_id = t1.cust_id and t3.order_id = t1.order_id
left join tmp_cust_browsing_behavior_info t4
on t4.cust_id = t1.cust_id and t4.order_id = t1.order_id
left join tmp_cust_communication_behavior_info t5
on t5.cust_id = t1.cust_id and t5.order_id = t1.order_id
where t2.is_inside_staff = 0 and t2.is_distribution = 0 and t2.is_scalper = 0;   -- 剔除内部会员/分销/黄牛
```
&emsp;&emsp;导出数据至本地，保存至文件“order_cust_info_202307.xlsx”。
&emsp;&emsp;各数据表的数据量、订单量、用户量预览如下。

|                    **table_name**                   | **data_size** | **cust_amount** | **order_amount** |
|:----------------------------------------------------|:-------------:|:---------------:|:----------------:|
| **tmp_order_202307**                            |     303527    |      160216     |      303527      |
| **tmp_order_cust_feature**                      |     160216    |      160216     |         \        |
| **tmp_cust_historical_order**                   |     303527    |      160216     |      303527      |
| **tmp_cust_browsing_info**                      |    29045750   |      160216     |      303527      |
| **tmp_cust_browsing_behavior_info**             |     303527    |      160216     |      303527      |
| **tmp_cust_communication_info**                 |     545407    |      160216     |      303527      |
| **tmp_cust_communication_behavior_info**        |     303527    |      160216     |      303527      |
| **tmp_order_cust_feature_behavior_info_202307** |     300381    |      159608     |      300381      |

<br>



## <font color = 'blue' >三、数据分析</font>
### 1. 数据预览
``` python
import pandas as pd

# 读取数据
order = pd.read_excel("./order_cust_info_202307.xlsx")
# 数据集预览
order.info()
```

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 300381 entries, 0 to 300380
Data columns (total 55 columns):
 #   Column                           Non-Null Count   Dtype         
---  ------                           --------------   -----         
 0   cust_id                          300381 non-null  int64         
 1   order_id                         300381 non-null  int64         
 2   create_time                      300381 non-null  datetime64[ns]
 3   executed_date                    183650 non-null  datetime64[ns]
 4   executed_flag                    300381 non-null  int64         
 5   order_date                       300381 non-null  datetime64[ns]
 6   route_id                         300381 non-null  int64         
 7   producttype_class_name           300363 non-null  object        
 8   star_level                       300381 non-null  int64         
 9   complaint_flag                   300381 non-null  int64         
 10  complaint_time                   7822 non-null    datetime64[ns]
 11  cust_type                        300381 non-null  int64         
 12  sex                              222354 non-null  object        
 13  age_group                        220637 non-null  object        
 14  user_province                    183696 non-null  object        
 15  user_city                        246039 non-null  object        
 16  access_channel_cnt               253466 non-null  float64       
 17  executed_orders_count            300381 non-null  int64         
 18  complaint_orders_count           300381 non-null  int64         
 19  decision_time                    300381 non-null  int64         
 20  browsing_flag                    300381 non-null  int64         
 21  first_operate_date               131872 non-null  datetime64[ns]
 22  browsing_days                    300381 non-null  int64         
 23  pv                               300381 non-null  int64         
 24  pv_daily_avg                     300381 non-null  float64       
 25  browsing_time                    300381 non-null  int64         
 26  browsing_time_daily_avg          300381 non-null  float64       
 27  browsing_products_cnt            300381 non-null  int64         
 28  browsing_products_cnt_daily_avg  300381 non-null  float64       
 29  lowest_price_avg                 105447 non-null  float64       
 30  browsing_vac_prd_cnt             300381 non-null  int64         
 31  browsing_single_prd_cnt          300381 non-null  int64         
 32  collect_coupons_cnt              300381 non-null  int64         
 33  comm_flag                        300381 non-null  int64         
 34  first_create_time                38732 non-null   datetime64[ns]
 35  comm_days                        300381 non-null  int64         
 36  comm_freq                        300381 non-null  int64         
 37  comm_freq_daily_avg              300381 non-null  float64       
 38  call_pct                         300381 non-null  float64       
 39  robot_pct                        300381 non-null  float64       
 40  officewx_pct                     300381 non-null  float64       
 41  chat_pct                         300381 non-null  float64       
 42  channels_cnt                     300381 non-null  int64         
 43  comm_time                        300381 non-null  int64         
 44  comm_time_daily_avg              300381 non-null  float64       
 45  calls_freq                       300381 non-null  int64         
 46  calls_freq_daily_avg             300381 non-null  float64       
 47  active_calls_freq                300381 non-null  int64         
 48  active_calls_pct                 300381 non-null  float64       
 49  chats_freq                       300381 non-null  int64         
 50  chats_freq_daily_avg             300381 non-null  float64       
 51  active_chats_freq                300381 non-null  int64         
 52  active_chats_pct                 300381 non-null  float64       
 53  vac_mention_num                  300381 non-null  int64         
 54  single_mention_num               300381 non-null  int64         
dtypes: datetime64[ns](6), float64(15), int64(29), object(5)
```

&emsp;&emsp;下面对数据做整理，提取出有用信息，对分类型数据进行替换，将文本类别替换为数字类别，格式转换为category，结果如下。sex、age_group、access_channel_cnt、lowest_price_min、lowest_price_max、lowest_price_avg存在缺失值。
``` python
# 提取有用的字段
data = pd.concat([order.iloc[:,0:2], order['executed_flag'], order['star_level'], order.iloc[:,11:14], order.iloc[:,16:20], order.iloc[:,22:33], order.iloc[:,35:]], axis=1)
data['sex'] = data['sex'].map({'男': 1, '女': 0})
data['age_group'] = data['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
data.iloc[:, 2:7] = data.iloc[:, 2:7].astype('category')
data.info()
```

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 300381 entries, 0 to 300380
Data columns (total 42 columns):
 #   Column                           Non-Null Count   Dtype   
---  ------                           --------------   -----   
 0   cust_id                          300381 non-null  int64   
 1   order_id                         300381 non-null  int64   
 2   executed_flag                    300381 non-null  category
 3   star_level                       300381 non-null  category
 4   cust_type                        300381 non-null  category
 5   sex                              222354 non-null  category
 6   age_group                        220637 non-null  category
 7   access_channel_cnt               253466 non-null  float64 
 8   executed_orders_count            300381 non-null  int64   
 9   complaint_orders_count           300381 non-null  int64   
 10  decision_time                    300381 non-null  int64   
 11  browsing_days                    300381 non-null  int64   
 12  pv                               300381 non-null  int64   
 13  pv_daily_avg                     300381 non-null  float64 
 14  browsing_time                    300381 non-null  int64   
 15  browsing_time_daily_avg          300381 non-null  float64 
 16  browsing_products_cnt            300381 non-null  int64   
 17  browsing_products_cnt_daily_avg  300381 non-null  float64 
 18  lowest_price_avg                 105447 non-null  float64 
 19  browsing_vac_prd_cnt             300381 non-null  int64   
 20  browsing_single_prd_cnt          300381 non-null  int64   
 21  collect_coupons_cnt              300381 non-null  int64   
 22  comm_days                        300381 non-null  int64   
 23  comm_freq                        300381 non-null  int64   
 24  comm_freq_daily_avg              300381 non-null  float64 
 25  call_pct                         300381 non-null  float64 
 26  robot_pct                        300381 non-null  float64 
 27  officewx_pct                     300381 non-null  float64 
 28  chat_pct                         300381 non-null  float64 
 29  channels_cnt                     300381 non-null  int64   
 30  comm_time                        300381 non-null  int64   
 31  comm_time_daily_avg              300381 non-null  float64 
 32  calls_freq                       300381 non-null  int64   
 33  calls_freq_daily_avg             300381 non-null  float64 
 34  active_calls_freq                300381 non-null  int64   
 35  active_calls_pct                 300381 non-null  float64 
 36  chats_freq                       300381 non-null  int64   
 37  chats_freq_daily_avg             300381 non-null  float64 
 38  active_chats_freq                300381 non-null  int64   
 39  active_chats_pct                 300381 non-null  float64 
 40  vac_mention_num                  300381 non-null  int64   
 41  single_mention_num               300381 non-null  int64   
dtypes: category(5), float64(15), int64(22)
```

&emsp;&emsp;绘制各数值型变量间的散点图矩阵，展示数据的分布情况，如下所示。
``` python
import matplotlib.pyplot as plt
import seaborn as sns

# 使用 seaborn 的 pairplot 绘制散点矩阵
sns.pairplot(data.iloc[:, 5:], diag_kind='hist', markers='+', plot_kws={'alpha': 0.8})
plt.show()
```
<div align="center"> 
  <a href="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/image.png">
    <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/image.png" alt="" width="1000" />
  </a>
</div>  


### 2. 描述统计
&emsp;&emsp;下面展示了成交订单和未成交订单的客户人群分布情况
``` python
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = 'SimHei' # 黑体

## 性别分布
cross_tab = pd.crosstab(order['sex'], order['executed_flag'])
# 绘制环形图
fig, ax = plt.subplots(figsize=(5, 5))
sex_labels = cross_tab.index
executed_labels = cross_tab.columns
data = cross_tab.values.flatten()
wedges, texts, autotexts = ax.pie(data, labels=[f'{sex} {exec}' for sex in sex_labels for exec in executed_labels],
                                   autopct='%1.2f%%', startangle=90, colors=plt.cm.Blues(np.linspace(0.2, 0.8, len(data))),
                                   wedgeprops={'edgecolor': 'w'})
for text in texts + autotexts:
    text.set_fontsize(12)
ax.set_title('Orders by Sex and Executed Flag')
centre_circle = plt.Circle((0, 0), 0.70, fc='white')
fig.gca().add_artist(centre_circle)
# 显示图表
plt.show()

## 年龄段分布
# 计算成交和未成交的数量和占比
executed_counts = order[order['executed_flag'] == 1]['age_group'].value_counts()
non_executed_counts = order[order['executed_flag'] == 0]['age_group'].value_counts()
total_counts = executed_counts + non_executed_counts
executed_ratios = executed_counts / total_counts
non_executed_ratios = non_executed_counts / total_counts
# 设置图形参数
plt.figure(figsize=(8, 5))
plt.rcParams['font.size'] = 12
# 绘制堆叠条形图
plt.bar(executed_counts.index, executed_counts, label='Executed')
plt.bar(non_executed_counts.index, non_executed_counts, bottom=executed_counts, label='Non-executed')
# 在图上显示各部分占比
for age_group, executed_ratio, non_executed_ratio in zip(executed_counts.index, executed_ratios, non_executed_ratios):
    plt.text(age_group, total_counts[age_group] / 2, f'{executed_ratio:.2%}', ha='center', va='center', color='white')
    plt.text(age_group, executed_counts[age_group] + non_executed_counts[age_group] / 2, f'{non_executed_ratio:.2%}', ha='center', va='center', color='white')
# 设置图例和标签
plt.legend()
plt.xlabel('Age Group')
plt.ylabel('Count')
plt.title('Executed vs Non-executed Orders by Age Group')
plt.xticks(rotation=45)
plt.tight_layout()
# 显示图表
plt.show()
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131458888.png" alt="性别分布" width="360" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131459467.png" alt="年龄段分布" width="600" />
</div>  

``` python
# 导出省份与城市的对应关系——中国省份_城市.txt
import requests
import json
header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36",
}
response = requests.get('https://j.i8tq.com/weather2020/search/city.js',headers=header)
result = json.loads(response.text[len('var city_data ='):])

# 省份名字的映射字典
province_mapping = {
    '北京': '北京市',
    '天津': '天津市',
    '河北': '河北省',
    '山西': '山西省',
    '内蒙古': '内蒙古自治区',
    '辽宁': '辽宁省',
    '吉林': '吉林省',
    '黑龙江': '黑龙江省',
    '上海': '上海市',
    '江苏': '江苏省',
    '浙江': '浙江省',
    '安徽': '安徽省',
    '福建': '福建省',
    '江西': '江西省',
    '山东': '山东省',
    '河南': '河南省',
    '湖北': '湖北省',
    '湖南': '湖南省',
    '广东': '广东省',
    '广西': '广西壮族自治区',
    '海南': '海南省',
    '重庆': '重庆市',
    '四川': '四川省',
    '贵州': '贵州省',
    '云南': '云南省',
    '西藏': '西藏自治区',
    '陕西': '陕西省',
    '甘肃': '甘肃省',
    '青海': '青海省',
    '宁夏': '宁夏回族自治区',
    '新疆': '新疆维吾尔自治区',
    '台湾': '台湾省',
    '香港': '香港特别行政区',
    '澳门': '澳门特别行政区'
}
each_province_data = {}
f = open('./中国省份_城市.txt',mode='w',encoding='utf-8')
for k,v in result.items():
    province = province_mapping.get(k, k)
    if k in ['上海', '北京', '天津', '重庆']:
        city = '，'.join(list(v[k].keys()))
    else:
        city = '，'.join(list(v.keys()))
    f.write(f'{province}_{city}\n')
    each_province_data[province] = city
f.close()
print(each_province_data)


# 用户所在省份分布
import copy
from pyecharts import options as opts
from pyecharts.charts import Map

order['user_city'] = order['user_city'].str.replace(r'市$', '', regex=True)
city = order['user_city'].value_counts()
city_list = [list(ct) for ct in city.items()]
def province_city():
    '''这是从接口里爬取的数据（不太准，但是误差也可以忽略不计！）'''
    area_data = {}
    with open('./中国省份_城市.txt', mode='r', encoding='utf-8') as f:
        for line in f:
            line = line.strip().split('_')
            area_data[line[0]] = line[1].split(',')
    province_data = []
    for ct in city_list:
        for k, v in area_data.items():
            for i in v:
                if ct[0] in i:
                    ct[0] = k
                    province_data.append(ct)
    area_data_deepcopy = copy.deepcopy(area_data)
    for k in area_data_deepcopy.keys():
        area_data_deepcopy[k] = 0
    for i in province_data:
        if i[0] in area_data_deepcopy.keys():
            area_data_deepcopy[i[0]] = area_data_deepcopy[i[0]] +i[1]
    province_data = [[k,v]for k,v in area_data_deepcopy.items()]
    best = max(area_data_deepcopy.values())
    return province_data,best
province_data,best = province_city()
#地图_中国地图（带省份）Map-VisualMap（连续型）
c1 = (
    Map()
    .add( "各省份下单用户量", province_data, "china")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="7月份下单用户所在省份的分布情况"),
        visualmap_opts=opts.VisualMapOpts(max_=int(best / 2)),
    )
    .render("map_china.html")
)


# 用户所在城市地图
city = order['user_city'].value_counts()
city_list = [list(ct) for ct in city.items()]
# 江苏省地级市
jiangsu_cities_mapping = {
    '南京': '南京市',
    '无锡': '无锡市',
    '徐州': '徐州市',
    '常州': '常州市',
    '苏州': '苏州市',
    '南通': '南通市',
    '连云港': '连云港市',
    '淮安': '淮安市',
    '盐城': '盐城市',
    '扬州': '扬州市',
    '镇江': '镇江市',
    '泰州': '泰州市',
    '宿迁': '宿迁市'
}
# 找出属于江苏省的城市列表并进行映射转换
jiangsu_cities_data = []
for city, value in city_list:
    if city in jiangsu_cities_mapping:
        jiangsu_cities_data.append((jiangsu_cities_mapping[city], value))
# 创建地图
c2 = (
    Map()
    .add("各城市下单用户量", jiangsu_cities_data, "江苏")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="江苏省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in jiangsu_cities_data])/2)),
    )
    .render("jiangsu_cities_map.html")
)

# 浙江省地级市
zhejiang_cities_mapping = {
    '杭州': '杭州市',
    '宁波': '宁波市',
    '温州': '温州市',
    '嘉兴': '嘉兴市',
    '湖州': '湖州市',
    '绍兴': '绍兴市',
    '金华': '金华市',
    '衢州': '衢州市',
    '舟山': '舟山市',
    '台州': '台州市',
    '丽水': '丽水市'
}
# 找出属于浙江省的城市列表并进行映射转换
zhejiang_cities_data = []
for city, value in city_list:
    if city in zhejiang_cities_mapping:
        zhejiang_cities_data.append((zhejiang_cities_mapping[city], value))
# 创建地图
c3 = (
    Map()
    .add("各城市下单用户量", zhejiang_cities_data, "浙江")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="浙江省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in zhejiang_cities_data])/2)),
    )
    .render("zhejiang_cities_map.html")
)

# 广东省地级市
guangdong_cities_mapping = {
    '广州': '广州市',
    '深圳': '深圳市',
    '珠海': '珠海市',
    '汕头': '汕头市',
    '韶关': '韶关市',
    '佛山': '佛山市',
    '江门': '江门市',
    '湛江': '湛江市',
    '茂名': '茂名市',
    '肇庆': '肇庆市',
    '惠州': '惠州市',
    '梅州': '梅州市',
    '汕尾': '汕尾市',
    '河源': '河源市',
    '阳江': '阳江市',
    '清远': '清远市',
    '东莞': '东莞市',
    '中山': '中山市',
    '潮州': '潮州市',
    '揭阳': '揭阳市',
    '云浮': '云浮市'
}
# 找出属于广东省的城市列表并进行映射转换
guangdong_cities_data = []
for city, value in city_list:
    if city in guangdong_cities_mapping:
        guangdong_cities_data.append((guangdong_cities_mapping[city], value))
# 创建地图
c4 = (
    Map()
    .add("各城市下单用户量", guangdong_cities_data, "广东")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="广东省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in guangdong_cities_data])/2)),
    )
    .render("guangdong_cities_map.html")
)

# 安徽省地级市
anhui_cities_mapping = {
    '合肥': '合肥市',
    '芜湖': '芜湖市',
    '蚌埠': '蚌埠市',
    '淮南': '淮南市',
    '马鞍山': '马鞍山市',
    '淮北': '淮北市',
    '铜陵': '铜陵市',
    '安庆': '安庆市',
    '黄山': '黄山市',
    '滁州': '滁州市',
    '阜阳': '阜阳市',
    '宿州': '宿州市',
    '六安': '六安市',
    '亳州': '亳州市',
    '池州': '池州市',
    '宣城': '宣城市'
}
# 找出属于安徽省的城市列表并进行映射转换
anhui_cities_data = []
for city, value in city_list:
    if city in anhui_cities_mapping:
        anhui_cities_data.append((anhui_cities_mapping[city], value))
# 创建地图
c5 = (
    Map()
    .add("各城市下单用户量", anhui_cities_data, "安徽")
    .set_global_opts(
        title_opts=opts.TitleOpts(title="安徽省下单用户分布"),
        visualmap_opts=opts.VisualMapOpts(max_=int(max([_[1] for _ in anhui_cities_data])/2)),
    )
    .render("anhui_cities_map.html")
)
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131500452.png" alt="" width="800" />
</div>  
&emsp;&emsp;由上图看出，上海、北京、江苏、浙江、广东、安徽等省份的用户分布较为密集，因此针对江苏、浙江、广东、安徽四个省份，绘制各省内地级市的人群分布情况，结果如下。
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131503417.png" alt="江苏省下单用户分布" width="400" />
  <p>江苏省下单用户分布</p>
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131503020.png" alt="浙江省下单用户分布" width="400" />
  <p>浙江省下单用户分布</p>
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131503505.png" alt="广东省下单用户分布" width="400" />
  <p>广东省下单用户分布</p>
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131503971.png" alt="安徽省下单用户分布" width="400" />
  <p>安徽省下单用户分布</p>
</div> 


### 3. 数据预处理
&emsp;&emsp;根据上述`data.info()`结果，发现特征中存在缺失值，因此，对'access_channel_cnt'的缺失值用0填充，对'lowest_price_min'、'lowest_price_max'、'lowest_price_avg'的缺失值用其平均值填充。
``` python
# 缺失值填充
data['access_channel_cnt'] = data['access_channel_cnt'].fillna(0) 
data['lowest_price_avg'] = data['lowest_price_avg'].fillna(data['lowest_price_avg'].mean()) 
```

&emsp;&emsp;然后，对数据进行变换。
&emsp;&emsp;首先，对数值型数据进行标准化处理，统一量纲；
&emsp;&emsp;其次，对分类数据进行独热编码（One-Hot Encoding），为了避免多重共线性问题，删除了编码后的第一个类别。在独热编码中，每个类别变量会被拆分成多个二进制变量，每个变量表示一个类别。这可以避免算法将类别之间的大小关系考虑为线性关系，从而更好地处理分类变量。
&emsp;&emsp;最后，将原始标签数据与编码后的数据、标准化后的数据合并，并将executed_flag标签进行重分，将成交的度假产品划分为1，其余划分为0，最终得到数据final_data。
``` python
from sklearn.preprocessing import StandardScaler

# 数值型变量——数据标准化
data_std = data.copy()
scaler = StandardScaler()
data_std.iloc[:, 7:] = scaler.fit_transform(data.iloc[:, 7:])

# 分类变量——独热编码
encoded_data = pd.get_dummies(data.iloc[:, 3:7], prefix=data.iloc[:, 3:7].columns, drop_first=True).astype(int)

# 将编码后的数据与原始数据合并
final_data = pd.concat([data_std.iloc[:,:3], encoded_data, data_std.iloc[:, 7:]], axis=1)
final_data

# 标签重分
# 1：1-度假产品成交，0：0 and 2-未成交 and 单资源产品成交
final_data['executed_flag'] = final_data['executed_flag'].replace({2: 0})
final_data['executed_flag'].unique()

final_data.info()
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131506522.png" alt="变换后数据预览" width="1000" />
</div>  

```
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 300381 entries, 0 to 300380
Data columns (total 54 columns):
 #   Column                           Non-Null Count   Dtype   
---  ------                           --------------   -----   
 0   cust_id                          300381 non-null  int64   
 1   order_id                         300381 non-null  int64   
 2   executed_flag                    300381 non-null  category
 3   star_level_1                     300381 non-null  int32   
 4   star_level_2                     300381 non-null  int32   
 5   star_level_3                     300381 non-null  int32   
 6   star_level_4                     300381 non-null  int32   
 7   star_level_5                     300381 non-null  int32   
 8   star_level_6                     300381 non-null  int32   
 9   star_level_7                     300381 non-null  int32   
 10  cust_type_1                      300381 non-null  int32   
 11  cust_type_2                      300381 non-null  int32   
 12  cust_type_3                      300381 non-null  int32   
 13  cust_type_4                      300381 non-null  int32   
 14  sex_1.0                          300381 non-null  int32   
 15  age_group_2.0                    300381 non-null  int32   
 16  age_group_3.0                    300381 non-null  int32   
 17  age_group_4.0                    300381 non-null  int32   
 18  age_group_5.0                    300381 non-null  int32   
 19  access_channel_cnt               300381 non-null  float64 
 20  executed_orders_count            300381 non-null  float64 
 21  complaint_orders_count           300381 non-null  float64 
 22  decision_time                    300381 non-null  float64 
 23  browsing_days                    300381 non-null  float64 
 24  pv                               300381 non-null  float64 
 25  pv_daily_avg                     300381 non-null  float64 
 26  browsing_time                    300381 non-null  float64 
 27  browsing_time_daily_avg          300381 non-null  float64 
 28  browsing_products_cnt            300381 non-null  float64 
 29  browsing_products_cnt_daily_avg  300381 non-null  float64 
 30  lowest_price_avg                 300381 non-null  float64 
 31  browsing_vac_prd_cnt             300381 non-null  float64 
 32  browsing_single_prd_cnt          300381 non-null  float64 
 33  collect_coupons_cnt              300381 non-null  float64 
 34  comm_days                        300381 non-null  float64 
 35  comm_freq                        300381 non-null  float64 
 36  comm_freq_daily_avg              300381 non-null  float64 
 37  call_pct                         300381 non-null  float64 
 38  robot_pct                        300381 non-null  float64 
 39  officewx_pct                     300381 non-null  float64 
 40  chat_pct                         300381 non-null  float64 
 41  channels_cnt                     300381 non-null  float64 
 42  comm_time                        300381 non-null  float64 
 43  comm_time_daily_avg              300381 non-null  float64 
 44  calls_freq                       300381 non-null  float64 
 45  calls_freq_daily_avg             300381 non-null  float64 
 46  active_calls_freq                300381 non-null  float64 
 47  active_calls_pct                 300381 non-null  float64 
 48  chats_freq                       300381 non-null  float64 
 49  chats_freq_daily_avg             300381 non-null  float64 
 50  active_chats_freq                300381 non-null  float64 
 51  active_chats_pct                 300381 non-null  float64 
 52  vac_mention_num                  300381 non-null  float64 
 53  single_mention_num               300381 non-null  float64 
dtypes: category(1), float64(35), int32(16), int64(2)
```


### 4. 选择变量
&emsp;&emsp;下图展示了特征间相关系数的热力图，发现浏览行为各特征间的相关度很高，沟通行为各特征间的相关度也很高，因此，推测可能存在多重共线性问题。
``` python
import matplotlib.pyplot as plt
import seaborn as sns

# 绘制热力图
plt.figure(figsize=(8, 8))
sns.heatmap(final_data.iloc[:, 1:21].corr(), cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()

plt.figure(figsize=(8, 8))
sns.heatmap(final_data.iloc[:, 21:].corr(), cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131510374.png" alt="所有特征热力图_1" width="600" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131510074.png" alt="所有特征热力图_2" width="600" />
</div> 

&emsp;&emsp;针对多重共线性问题，进行逐步回归，筛选出关键特征。
``` python
#################################### 逐步回归
def stepwise_select(data,label,cols_all,method='forward'):
    '''
    args:
        data：数据源，df
        label：标签，str
        cols_all：逐步回归的全部字段
        methrod：方法，forward:向前，backward:向后，both:双向
    return:
        select_col：最终保留的字段列表，list 
        summary：模型参数
        AIC：aic
    '''
    import statsmodels.api as sm
    
    ######################## 1.前向回归
    # 前向回归：从一个变量都没有开始，一个变量一个变量的加入到模型中，直至没有可以再加入的变量结束
    if method == 'forward':  
        add_col = [] 
        AIC_None_value = np.inf
        while cols_all:
            # 单个变量加入，计算aic
            AIC = {}
            for col in cols_all:
                print(col)
                X_col = add_col.copy()
                X_col.append(col)
                X = sm.add_constant(data[X_col])
                y = data[label]
                LR = sm.Logit(y, X).fit()
                AIC[col] = LR.aic
            AIC_min_value = min(AIC.values())   
            AIC_min_key = min(AIC,key=AIC.get)
            # 如果最小的aic小于不加该变量时的aic，则加入变量，否则停止
            if AIC_min_value < AIC_None_value:
                cols_all.remove(AIC_min_key)
                add_col.append(AIC_min_key)
                AIC_None_value = AIC_min_value
            else:
                break
        select_col = add_col
    ######################## 2.后向回归
    # 从全部变量都在模型中开始，一个变量一个变量的删除，直至没有可以再删除的变量结束
    elif method == 'backward': 
        p = True  
        # 全部变量，一个都不剔除，计算初始aic
        X_col = cols_all.copy()
        X = sm.add_constant(data[X_col])
        y = data[label]
        LR = sm.Logit(y, X).fit()
        AIC_None_value = LR.aic        
        while p:      
           # 删除一个字段提取aic最小的字段
           AIC = {}
           for col in cols_all:
               print(col)
               X_col = [i for i in cols_all if i!=col]
               X = sm.add_constant(data[X_col])
               LR = sm.Logit(y, X).fit()
               AIC[col] = LR.aic
           AIC_min_value = min(AIC.values()) 
           AIC_min_key = min(AIC, key=AIC.get)  
           # 如果最小的aic小于不删除该变量时的aic，则删除该变量，否则停止
           if AIC_min_value < AIC_None_value:
               cols_all.remove(AIC_min_key)
               AIC_None_value = AIC_min_value
               p = True
           else:
               break 
        select_col = cols_all             
    ######################## 3.双向回归
    elif method == 'both': 
        p = True
        add_col = []
        # 全部变量，一个都不剔除，计算初始aic
        X_col = cols_all.copy()
        X = sm.add_constant(data[X_col])
        y = data[label]
        LR = sm.Logit(y, X).fit()
        AIC_None_value = LR.aic        
        while p: 
            # 删除一个字段提取aic最小的字段
            AIC={}
            for col in cols_all:
                print(col)
                X_col = [i for i in cols_all if i!=col]
                X = sm.add_constant(data[X_col])
                LR = sm.Logit(y, X).fit()
                AIC[col] = LR.aic     
            AIC_min_value = min(AIC.values())
            AIC_min_key = min(AIC, key=AIC.get)
            if len(add_col) == 0: # 第一次只有删除操作，不循环加入变量
                if AIC_min_value < AIC_None_value:
                    cols_all.remove(AIC_min_key)
                    add_col.append(AIC_min_key)
                    AIC_None_value = AIC_min_value
                    p = True
                else:
                    break
            else:
                # 单个变量加入，计算aic
                for col in add_col:
                    print(col)
                    X_col = cols_all.copy()
                    X_col.append(col)
                    X = sm.add_constant(data[X_col])
                    LR = sm.Logit(y, X).fit()
                    AIC[col] = LR.aic
                AIC_min_value = min(AIC.values())
                AIC_min_key = min(AIC, key=AIC.get)
                if AIC_min_value < AIC_None_value:
                    # 如果aic最小的字段在添加变量阶段产生，则加入该变量，如果aic最小的字段在删除阶段产生,则删除该变量
                    if AIC_min_key in add_col:
                        cols_all.append(AIC_min_key)
                        add_col = list(set(add_col)-set(AIC_min_key))
                        p = True                    
                    else: 
                        cols_all.remove(AIC_min_key)
                        add_col.append(AIC_min_key)
                        p = True
                    AIC_None_value = AIC_min_value
                else:
                    break
        select_col = cols_all 
    ######################## 模型
    X = sm.add_constant(data[select_col])
    LR = sm.Logit(y, X).fit()    
    summary = LR.summary()
    AIC = LR.aic
    return select_col,summary,AIC


# 逐步回归
select_col, summary, AIC = stepwise_select(final_data, final_data.columns.tolist()[2], final_data.columns.tolist()[3:], method='both')

# 不显著特征
list(set(final_data.columns) - set(select_col) - set(['executed_flag', 'cust_id', 'order_id']))

print(summary)
```
&emsp;&emsp;根据定义的逐步回归函数，对数据做双向逐步回归，筛选对目标变量影响显著的特征，对标签影响不显著的特征有：
    ```['chat_pct', 'active_calls_pct']```

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131516921.png" alt="逐步回归结果_1" width="600" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131516545.png" alt="逐步回归结果_2" width="600" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131516703.png" alt="逐步回归结果_3" width="600" />
</div> 

&emsp;&emsp;回归方程结果表明，逐步回归后特征间仍然存在多重共线性，下面通过计算方差膨胀因子VIF值筛选变量。一般情况下，当VIF > 10时，存在严重多重共线性，故此处删除VIF大于10的特征，保留其余特征，结果如下。
``` python
def calculate_vif(data):
    vif_data = pd.DataFrame()
    vif_data["feature"] = data.columns
    vif_data["VIF"] = [variance_inflation_factor(data.values, i) for i in range(len(data.columns))]
    return vif_data

def stepwise_vif_selection(data, target, significance_level=0.05):
    selected_features = list(data.columns)
    dropped = True

    while dropped:
        dropped = False
        vif_data = calculate_vif(data[selected_features])
        max_vif_feature = vif_data.loc[vif_data['VIF'].idxmax()]['feature']
        if vif_data['VIF'].max() > 10:
            print(f"Removing feature with high VIF: {max_vif_feature}")
            selected_features.remove(max_vif_feature)
            dropped = True
    
    return selected_features

# final_data 是数据集，'executed_flag' 是目标变量
selected_features = stepwise_vif_selection(final_data[select_col], 'executed_flag')
print(selected_features)
```

```
Removing feature with high VIF: calls_freq_daily_avg
Removing feature with high VIF: comm_time_daily_avg
Removing feature with high VIF: chats_freq
Removing feature with high VIF: pv
Removing feature with high VIF: comm_freq
Removing feature with high VIF: comm_time
Removing feature with high VIF: comm_freq_daily_avg
```

&emsp;&emsp;Significant variable为筛选出的显著特征，Insignificant variable为剔除的非显著特征。

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-zc7g{background-color:#a6dde0;border-color:#000000;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-ce3n{background-color:#72bcc0;border-color:inherit;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-fv5r{background-color:#72bcc0;border-color:inherit;color:#000000;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-7btt{border-color:inherit;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0pky{border-color:inherit;text-align:left;vertical-align:top}
</style>
<table class="tg">
<thead>
  <tr>
    <th class="tg-ce3n" rowspan="2">特征维度</th>
    <th class="tg-fv5r" colspan="2">Significant variable</th>
    <th class="tg-fv5r" colspan="2">Insignificant variable</th>
  </tr>
  <tr>
    <th class="tg-zc7g">name</th>
    <th class="tg-zc7g">comment</th>
    <th class="tg-zc7g">name</th>
    <th class="tg-zc7g">comment</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-7btt" rowspan="17">基本信息</td>
    <td class="tg-0pky">star_level_1</td>
    <td class="tg-0pky">下单时的会员等级：1</td>
    <td class="tg-0pky">chat_pct</td>
    <td class="tg-0pky">在线客服占比（在线客服沟通次数/总沟通次数）</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_2</td>
    <td class="tg-0pky">下单时的会员等级：2</td>
    <td class="tg-0pky">active_calls_pct</td>
    <td class="tg-0pky">用户主动通话占比 = 用户主动通话频数 / 通话频数</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_3</td>
    <td class="tg-0pky">下单时的会员等级：3</td>
    <td class="tg-0pky">calls_freq_daily_avg</td>
    <td class="tg-0pky">每日平均通话频数：电话+智能外呼</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_4</td>
    <td class="tg-0pky">下单时的会员等级：4</td>
    <td class="tg-0pky">comm_time_daily_avg</td>
    <td class="tg-0pky">每日平均沟通时长</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_5</td>
    <td class="tg-0pky">下单时的会员等级：5</td>
    <td class="tg-0pky">chats_freq</td>
    <td class="tg-0pky">聊天频数：企微聊天+在线客服</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_6</td>
    <td class="tg-0pky">下单时的会员等级：6</td>
    <td class="tg-0pky">pv</td>
    <td class="tg-0pky">用户总pv</td>
  </tr>
  <tr>
    <td class="tg-0pky">star_level_7</td>
    <td class="tg-0pky">下单时的会员等级：7</td>
    <td class="tg-0pky">comm_freq</td>
    <td class="tg-0pky">总沟通次数</td>
  </tr>
  <tr>
    <td class="tg-0pky">cust_type_1</td>
    <td class="tg-0pky">会员类型：1-老带新新会员</td>
    <td class="tg-0pky">comm_time</td>
    <td class="tg-0pky">总沟通时长（特指电话、智能外呼）</td>
  </tr>
  <tr>
    <td class="tg-0pky">cust_type_2</td>
    <td class="tg-0pky">会员类型：2-新会员(新客户)</td>
    <td class="tg-0pky">comm_freq_daily_avg</td>
    <td class="tg-0pky">每日平均沟通次数</td>
  </tr>
  <tr>
    <td class="tg-0pky">cust_type_3</td>
    <td class="tg-0pky">会员类型：3-老会员(新客户)</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">cust_type_4</td>
    <td class="tg-0pky">会员类型：4-老会员老客户</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">sex_1.0</td>
    <td class="tg-0pky">会员性别：女</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">age_group_2.0</td>
    <td class="tg-0pky">会员年龄段：16~25岁</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">age_group_3.0</td>
    <td class="tg-0pky">会员年龄段：26~35岁</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">age_group_4.0</td>
    <td class="tg-0pky">会员年龄段：36~45岁</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">age_group_5.0</td>
    <td class="tg-0pky">会员年龄段：大于46岁</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">access_channel_cnt</td>
    <td class="tg-0pky">可触达渠道的个数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-7btt" rowspan="2">订单信息</td>
    <td class="tg-0pky">executed_orders_count</td>
    <td class="tg-0pky">历史15天内成交订单数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">complaint_orders_count</td>
    <td class="tg-0pky">历史15天内投诉订单数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-7btt"></td>
    <td class="tg-0pky">decision_time</td>
    <td class="tg-0pky">历史15天内决策时间</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-7btt" rowspan="10">浏览行为</td>
    <td class="tg-0pky">browsing_days</td>
    <td class="tg-0pky">历史浏览天数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">pv_daily_avg</td>
    <td class="tg-0pky">每日平均pv</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_time</td>
    <td class="tg-0pky">用户总浏览时间</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_time_daily_avg</td>
    <td class="tg-0pky">每日平均浏览时间</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_products_cnt</td>
    <td class="tg-0pky">历史浏览产品量</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_products_cnt_daily_avg</td>
    <td class="tg-0pky">每日平均浏览产品量</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">lowest_price_avg</td>
    <td class="tg-0pky">历史浏览产品（最低价）平均价</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_vac_prd_cnt</td>
    <td class="tg-0pky">历史浏览的度假产品数量</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">browsing_single_prd_cnt</td>
    <td class="tg-0pky">历史浏览的单资源产品数量</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">collect_coupons_cnt</td>
    <td class="tg-0pky">优惠券领券个数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-7btt" rowspan="12">沟通行为</td>
    <td class="tg-0pky">comm_days</td>
    <td class="tg-0pky">历史沟通天数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">call_pct</td>
    <td class="tg-0pky">电话呼入呼出占比（电话呼入呼出/总沟通次数）</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">robot_pct</td>
    <td class="tg-0pky">智能外呼占比</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">officewx_pct</td>
    <td class="tg-0pky">企微聊天占比</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">channels_cnt</td>
    <td class="tg-0pky">历史沟通渠道数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">calls_freq</td>
    <td class="tg-0pky">通话频数：电话+智能外呼</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">active_calls_freq</td>
    <td class="tg-0pky">用户主动通话频数：电话+智能外呼</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">chats_freq_daily_avg</td>
    <td class="tg-0pky">每日平均聊天频数：企微聊天+在线客服</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">active_chats_freq</td>
    <td class="tg-0pky">用户主动聊天频数：企微聊天+在线客服</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">active_chats_pct</td>
    <td class="tg-0pky">用户主动聊天占比 = 用户主动聊天频数 / 聊天频数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">vac_mention_num</td>
    <td class="tg-0pky">沟通过程中度假产品的总提及次数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
  <tr>
    <td class="tg-0pky">single_mention_num</td>
    <td class="tg-0pky">沟通过程中度假产品的总提及次数</td>
    <td class="tg-0pky"></td>
    <td class="tg-0pky"></td>
  </tr>
</tbody>
</table>

&emsp;&emsp;下面绘制了热力图，以查看筛选后的特征之间的相关关系。
``` python
import matplotlib.pyplot as plt
import seaborn as sns

# 绘制热力图
plt.figure(figsize=(8, 8))
sns.heatmap(final_data[select_col[:19]].corr(), annot=True, annot_kws={"size": 8}, fmt=".2f", cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()

plt.figure(figsize=(8, 8))
sns.heatmap(final_data[select_col[19:]].corr(), annot=True, annot_kws={"size": 8}, fmt=".2f", cmap="RdBu_r")
plt.title("Correlation Heatmap")
plt.show()
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131540802.png" alt="筛选后特征热力图_1" width="600" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131540870.png" alt="筛选后特征热力图_2" width="600" />
</div> 
&emsp;&emsp;热力图显示，筛选后的浏览行为相关特征（pv、browsing_time等）之间相关性较高，沟通行为相关特征（comm_days、comm_freq_daily_avg等）之间相关性较高，其他各特征之间关联度较低。

&emsp;&emsp;对筛选后的变量进行差异分析。对分类变量（star_level、cust_type、sex、age_group、access_channel_cnt）进行非参数卡方检验，并给出了成交量最多和最少的变量水平；对其他连续型变量进行方差分析，并计算了成交订单和非成交订单的组内平均值，结果如下。
``` python
# 对于分类变量，使用卡方检验
from scipy.stats import chi2_contingency

results = []
for var in ['star_level', 'cust_type', 'sex', 'age_group', 'access_channel_cnt']:
    contingency_table = pd.crosstab(data[var], data['executed_flag'])
    chi2, p, dof, expected = chi2_contingency(contingency_table)
    p_value_significant = "显著" if p < 0.05 else "不显著"
    results.append({'Variable': var,
                    'P-value': p,
                    'Significant': p_value_significant,
                    'Executed Most Level': contingency_table[1].idxmax(),
                    'Executed Least Level': contingency_table[1].idxmin()})
print(pd.DataFrame(results))


# 对于连续性变量，使用方差分析
from scipy.stats import f_oneway

unselect = set(final_data.columns) - set(select_col)
variables = list(set(varnames) - unselect)
variables = sorted(variables, key=lambda x: colnames.index(x))
results = []
for var in variables[5:]:
    executed_data = data[data['executed_flag'] == 1][var]
    unexecuted_data = data[data['executed_flag'] == 0][var]
    result = f_oneway(executed_data, unexecuted_data)
    var_mean = data.groupby('executed_flag')[var].mean()
    results.append({'Variable': var,
                    'P-value': result.pvalue,
                    'Significant': p_value_significant,
                    'Executed Mean': round(var_mean.loc[1], 4),
                    'Unexecuted Mean': round(var_mean.loc[0], 4)})
pd.DataFrame(results)
```
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131542368.png" alt="分类变量的卡方检验结果" width="600" />
  <p>分类变量的卡方检验结果</p>
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131542601.png" alt="连续变量的方差分析结果" width="600" />
  <p>连续变量的方差分析结果</p>
</div> 
&emsp;&emsp;结果表明，在成交的订单中，下单时会员等级为0、老会员老客户、女性、36~45岁，可触达渠道数为1的群体的订单成交量最多，成交订单组内的平均pv在25次左右、平均历史浏览总时间在1421s左右、平均历史浏览产品最低价的平均价格在1386元左右的群体订单成交意愿最高，其它同理。


### 5. 随机森林分类预测
&emsp;&emsp;基于随机森林，对处理后的数据建进分类预测。
``` python
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix

X = final_data[selected_features]
y = final_data['executed_flag']

# 划分训练集和测试集
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 构建随机森林分类器
rf_classifier = RandomForestClassifier(n_estimators=100, random_state=42)

# 在训练集上训练模型
rf_classifier.fit(X_train, y_train)

# 在测试集上进行预测
y_pred = rf_classifier.predict(X_test)

# 计算模型评价指标
print("AUC：", round(roc_auc_score(y_test, y_pred), 4))
print(classification_report(y_test, y_pred))
print("混淆矩阵：", confusion_matrix(y_test, y_pred))

# 保存模型
joblib.dump(rf_classifier, './rf_classifier_model.joblib')


# 获取特征重要性
feature_importances = rf_classifier.feature_importances_
# 对特征重要性进行排序
sorted_indices = np.argsort(feature_importances)[::-1]  # 降序排列
# 可视化特征重要性
plt.figure(figsize=(10, 6))
plt.rcParams['font.size'] = 12
plt.bar(range(X_train.shape[1]), feature_importances[sorted_indices])
plt.xticks(range(X_train.shape[1]), X_train.columns[sorted_indices], rotation=90)
plt.xlabel('Feature')
plt.ylabel('Importance')
plt.title('Feature Importance')
plt.tight_layout()
plt.show()
```

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-baqh{text-align:center;vertical-align:top}
.tg .tg-t4dz{background-color:#B0D4CC;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-v658{background-color:#72B7EF;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-zm63{background-color:#B7DCFB;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-50tr{background-color:#D8ECE7;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0lax{text-align:left;vertical-align:top}
</style>
<table class="tg" style="undefined;table-layout: fixed; width: 268px">
<colgroup>
<col style="width: 66px">
<col style="width: 65px">
<col style="width: 69px">
<col style="width: 68px">
</colgroup>
<thead>
  <tr>
    <th class="tg-amwm" colspan="2" rowspan="2"><span style="font-weight:bold">混淆矩阵</span></th>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">预测值</span></th>
  </tr>
  <tr>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-t4dz" rowspan="2"><span style="font-weight:bold">真实值</span></td>
    <td class="tg-50tr"><span style="font-weight:bold">0</span></td>
    <td class="tg-baqh">43886</td>
    <td class="tg-baqh">2439</td>
  </tr>
  <tr>
    <td class="tg-50tr"><span style="font-weight:bold">1</span></td>
    <td class="tg-baqh">3511</td>
    <td class="tg-baqh">10241</td>
  </tr>
</tbody>
</table>

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131602783.png" alt="模型评价指标结果" width="500" />
  <p>模型评价指标结果</p>
</div> 
&emsp;&emsp;随机森林模型的AUC值为0.846，预测准确率为90%，精确率为81%，召回率为74%，F1值为0.77。

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131603739.png" alt="模型特征重要性排序" width="600" />
</div> 
&emsp;&emsp;将特征对模型的重要性进行排序，上图显示了每个特征对于该模型的预测能力的贡献程度。通常来说，具有较高重要性分数的特征对模型的预测能力有更大的影响。结果表明access_channel_cnt对度假产品成交与否的影响程度最大，其次是cust_type_4、lowest_price_avg、browsing_time等特征，cust_type_1对度假产品成交与否的影响程度最小。


### 6. 结论与建议
&emsp;&emsp;本文针对7月份用户的订单数据，综合用户的个人基本信息和历史15天的行为数据（包括浏览行为和沟通行为），对接下来一天用户是否会下单度假产品并成交进行预测。首先，对不同用户群体进行了描述性统计和差异分析，意在说明哪个群体的订单成交量较多、成交意愿更大；其次，由于选择的特征量较多，且相关度较高，通过逐步回归和方差膨胀因子筛选出对标签影响显著的变量，以解决多重共线性问题；最后，利用随机森林对数据建立模型，对用户在接下来一天是否会下单度假产品并成交进行预测。
1. 在7月份的订单中，女性群体用户的下单和成交意愿均高于男性群体用户；相较于其它年龄段，26~45岁用户群体的下单和成交意愿最高，其中36~45岁用户群体的成交概率更高；下单用户主要分布在上海、北京、江苏、浙江、广东、安徽，其中又主要分布在各省的省会城市，例如南京市、杭州市、广州市、合肥市，说明这些地区的用户最活跃，有更大概率下单。
2. 对成交用户和未成交用户的群体进行差异分析，结果表明，下单时会员等级为0、老会员老客户、36~45岁、可触达渠道数为1个的女性群体的订单成交量最多；历史有成交订单、决策时间3天、PV25次、历史浏览总时间在1421s左右、平均每日浏览时间355s、历史浏览产品最低价的平均价格1386元、电话和智能外呼的通话总时长70s的群体订单成交意愿相对较高。
3. 对用户订单成交结果有显著影响的特征有：用户下单时会员等级、会员类型、性别、年龄段、用户可触达渠道的数量、历史订单成交量、历史订单投诉量、决策时间、PV/每日平均PV、浏览时长/每日平均浏览时间、浏览产品的平均价格、优惠券领取量、沟通天数、每日平均沟通次数、各渠道沟通数占比、通话时长和频数、用户主动通话频数、每日平均聊天频数、用户主动聊天频数及其占比。
4. 基于用户的基本信息和历史15天的行为数据（包括浏览行为和沟通行为）预测下一天该用户是否会成交，使用随机森林进行分类预测，模型的AUC值为0.7042。  
  预测的准确率达到了90%，表明模型正确预测了90%的订单的成交与否；  
  精确率为81%，表明在模型预测的成交订单中，有75%是实际成交的；  
  召回率为89%，表明在所有实际成交的订单中，模型正确预测为成交订单的比例占89%；  
  F1值为0.81，表明该随机森林模型在平衡预测准确性和捕获能力方面表现良好。  

&emsp;&emsp;因此，可以对上述结论1、2中的群体、以及模型预测为“成交”的用户进行精准化维护，这部分用户下单并成交的概率相对较高。


### 7. 模型测试
&emsp;&emsp;**取数：** 2023-08-09有浏览或沟通记录的用户的历史15天的行为数据、历史订单成交量和投诉量，以及个人基本信息；
&emsp;&emsp;**目的：** 预测该部分用户在2023-08-10是否有成交订单。

``` sql
-- 查询2023-08-09有浏览或沟通行为的用户ID
drop table if exists tmp_cust_id_20230809;
create table tmp_cust_id_20230809 as
select cust_id
from (
    select distinct cast(vt_mapuid as bigint) cust_id from dw.kn1_traf_app_day_detail   -- APP流量
    where dt = '20230809'
    union
    select distinct cast(vt_mapuid as bigint) cust_id from dw.kn1_traf_day_detail       -- PC/M站流量
    where dt = '20230809'
    union
    select distinct b.user_id cust_id
    from (
        select case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num
        from dw.kn1_call_tel_order_detail                                   -- 电话呼入呼出
        where dt = '20230809'
    ) a
    left join (
        select user_id, feature_value as tel_num from dw.ol_rs_meta_feature_base_information
        where three_class_name = '手机号'
    ) b on b.tel_num = a.tel_num and a.tel_num is not null
    union
    select distinct cust_id from dw.kn1_sms_robot_outbound_call_detail      -- 智能外呼
    where dt = '20230809'
    union
    select distinct cust_id from dw.kn1_officeweixin_sender_cust_content    -- 企微聊天
    where dt = '20230809'
    union
    select distinct cust_id from dw.kn1_autotask_user_acsessed_chat         -- 在线客服
    where dt = '20230809'
) t 
where cust_id between 1 and 1000000001;
```
``` sql
-- 订购用户画像
drop table if exists tmp_order_cust_feature;
create table tmp_order_cust_feature as
select
    a.cust_id,                       -- 会员ID
    b.cust_type,                     -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    b.cust_level,                    -- 会员星级
    b.is_inside_staff,               -- 是否内部员工:0否 1是
    b.is_distribution,               -- 是否分销:0否 1是
    b.is_scalper,                    -- 是否黄牛:0否 1是
    rs1.sex,                         -- 用户性别
    rs2.age,                         -- 用户年龄
    rs3.age_group,                   -- 用户年龄段
    rs4.tel_num,                     -- 用户手机号
    rs5.user_city,                   -- 用户所在城市
    rs6.user_province,               -- 用户所在省份
    rs7.access_channel_cnt,          -- 可触达渠道的个数
    rs7.access_channels              -- 可触达渠道：push/企业微信/短信
from tmp_cust_id_20230809 a
left join (
    select
        cust_id, cust_type, cust_level, is_inside_staff, is_distribution, is_scalper
    from dw.kn1_usr_cust_attribute
    where cust_id in (select * from tmp_cust_id_20230809)
) b on b.cust_id = a.cust_id
left join (
    -- 会员基本信息-性别
    select user_id, feature_name as sex
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '性别'
        and user_id in (select * from tmp_cust_id_20230809)
) rs1 on a.cust_id = rs1.user_id
left join (
    -- 会员基本信息-年龄
    select user_id, feature_value as age
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄'
        and user_id in (select * from tmp_cust_id_20230809)
) rs2 on a.cust_id = rs2.user_id
left join (
    -- 会员基本信息-年龄段
    select user_id, feature_name as age_group
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '年龄段'
        and user_id in (select * from tmp_cust_id_20230809)
) rs3 on a.cust_id = rs3.user_id
left join (
    -- 会员基本信息-手机号
    select user_id, feature_value as tel_num
    from dw.ol_rs_meta_feature_base_information
    where three_class_name = '手机号'
        and user_id in (select * from tmp_cust_id_20230809)
) rs4 on a.cust_id = rs4.user_id
left join (
    -- 基本属性--基本信息-用户所在城市
    select user_id, feature_name as user_city, feature_value as user_city_key
    from dw.ol_rs_meta_feature_basic_info
    where three_class_name = '所在城市'
        and user_id in (select * from tmp_cust_id_20230809)
) rs5 on a.cust_id = rs5.user_id
left join (
    -- 用户所在省份
    select city_key, parent_province_name as user_province
    from dw.kn1_pub_all_city_key
) rs6 on rs5.user_city_key = rs6.city_key
left join (
    -- 基本属性--个人信息-可触达渠道：push/企业微信/短信
    select 
        user_id, 
        count(1) as access_channel_cnt,
        concat_ws(',', collect_set(three_class_name)) as access_channels
    from dw.ol_rs_meta_feature_basic_info_access_channel
    where feature_value = 1 and user_id in (select * from tmp_cust_id_20230809)
    group by user_id
) rs7 on a.cust_id = rs7.user_id;
```
``` sql
-- 计算用户在下单前15内成交订单数、投诉订单数
drop table if exists tmp_cust_historical_order;
create table tmp_cust_historical_order as
select
    cust_id,
    sum(if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, 1, 0)) executed_orders_count,   -- 历史15天内成交订单数
    sum(complaint_flag) complaint_orders_count              -- 历史15天内投诉订单数
from dw.kn2_ord_order_detail_all
where dt = '20230809' and create_time >= '2023-07-26'   -- 统计截止2023-08-09历史15天的订单
    and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
    and cust_id in (select * from tmp_cust_id_20230809)
group by cust_id;
```
``` sql
--  用户的浏览行为信息
drop table if exists tmp_cust_browsing_info_20230809;
create table tmp_cust_browsing_info_20230809 as
select
    cast(a.vt_mapuid as bigint) cust_id,                    -- 会员ID
    a.operate_date,                                         -- 用户访问日期
    a.product_id,                                           -- 产品ID
    a.residence_time,                                       -- 页面停留时间
    a.visitor_trace,                                        -- 访客标记
    b.lowest_price,                                         -- 产品最低价
    c.producttype_class_name,                               -- 浏览产品大类
    nvl(d.collect_coupons_status, 0) collect_coupons_status -- 优惠券领取状态 1-成功 0-未领取/失败
from (
    -- 流量域
    select distinct vt_mapuid, to_date(operate_time) operate_date, product_id, residence_time, visitor_trace
    from dw.kn1_traf_app_day_detail
    where dt between '20230726' and '20230809'
        and vt_mapuid in (select * from tmp_cust_id_20230809)
    union
    select distinct vt_mapuid, to_date(operate_time) operate_date, product_id, residence_time, visitor_trace
    from dw.kn1_traf_day_detail
    where dt between '20230726' and '20230809'
        and vt_mapuid in (select * from tmp_cust_id_20230809)
) a
left join (
    -- 产品最低价
    select distinct route_id, lowest_price
    from dw.kn1_prd_route
) b on b.route_id = a.product_id
left join (
    select 
        distinct route_id,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = a.product_id
left join (
    -- 用户领券情况
    select distinct cust_id, collect_coupons_status, to_date(operate_time) operate_date
    from dw.ods_crmmkt_mkt_scene_clear_intention_cust_transform
    where to_date(operate_time) between '20230726' and '20230809'
        and collect_coupons_status = 1 and cust_id in (select * from tmp_cust_id_20230809)
) d on d.cust_id = a.vt_mapuid and d.operate_date = a.operate_date;
```
``` sql
-- 用户的沟通行为信息
drop table if exists tmp_cust_communication_info_20230809;
create table tmp_cust_communication_info_20230809 as
select
    cust_id,                              -- 会员编号
    create_time,                          -- 沟通日期
    channel,                              -- 沟通渠道名称
    channel_id,                           -- 沟通渠道ID 1-电话呼入呼出 2-智能外呼 3-企微聊天 4-在线客服
    comm_duration,                        -- 沟通持续时长
    comm_num,                             -- 沟通量：通话量/聊天数
    active_comm_num,                      -- 用户主动进行沟通的数量
    vac_mention_num,                      -- 沟通过程中度假产品的提及次数
    single_mention_num                    -- 沟通过程中单资源产品的提及次数
from (
    -- 电话明细
    select distinct cust_id, create_time, comm_duration, comm_num, active_comm_num, vac_mention_num, single_mention_num, '电话呼入呼出' as channel, 1 as channel_id
    from (
        select
            cust_id, create_time,
            sum(status_time) comm_duration,     -- 通话总时长
            count(1) comm_num,                  -- 接通量
            sum(case when calldir='呼入' then 1 else 0 end) active_comm_num,   -- 用户呼入量
            sum(case when producttype_class_name like '度假产品' then 1 else 0 end) vac_mention_num,
            sum(case when producttype_class_name like '单资源产品' then 1 else 0 end) single_mention_num
        from (
            select cust_id, tel_num
            from tmp_order_cust_feature
        ) t1
        left join (
            select
                case when length(cust_tel_no) > 11 then substr(cust_tel_no, -11) else cust_tel_no end tel_num,
                to_date(status_start_time) create_time,
                status_time, calldir, order_id
            from dw.kn1_call_tel_order_detail
            where dt between '20230726' and '20230809' and status='通话'    -- 统计接通用户数据        
        ) t2 on t2.tel_num = t1.tel_num
        left join (
            select order_id, route_id, book_city from dw.kn2_ord_order_detail_all
            where dt = '20230731' and create_time >= '2023-07-01'
        ) t3 on t3.order_id = t2.order_id
        left join (
            select distinct route_id, book_city,
                case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
            from dw.kn2_dim_route_product_sale
        ) t4 on t4.route_id = t3.route_id and t4.book_city = t3.book_city
        where cust_id in (select * from tmp_cust_id_20230809)
        group by cust_id, create_time
    ) t
    union all
    -- 机器人外呼明细
    select
        cust_id, to_date(answer_time) create_time,
        sum(call_time) comm_duration,           -- 通话总时长
        count(1) comm_num,                      -- 接通量
        sum(case when generate_task_is = 1 and label <> 'D' then 1 else 0 end) active_comm_num,   -- 命中用户意向量
        0 as vac_mention_num, 0 as single_mention_num,
        '智能外呼' as channel, 2 as channel_id
    from dw.kn1_sms_robot_outbound_call_detail
    where dt between '20230726' and '20230809' and answer_flag = 1      -- 统计接通用户数据
        and cust_id in (select * from tmp_cust_id_20230809)
    group by cust_id, to_date(answer_time)
    union all
    -- 企微聊天明细
    select 
        cust_id, to_date(msg_time) create_time,
        null as comm_duration,                  -- 聊天时长：null
        count(msg_time) comm_num,               -- 发送消息数
        sum(case when type=1 then 1 else 0 end) active_comm_num,   -- 用户主动聊天数
        sum(case when contact like '%商旅%' or contact like '%跟团%' or contact like '%自驾%' or contact like '%自助%'
                or contact like '%目的地服务%' or contact like '%签证%' or contact like '%团队%' or contact like '%定制%'
                or contact like '%游轮%' or contact like '%旅拍%' or contact like '%游%' or contact like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when contact like '%火车票%' or contact like '%机票%' or contact like '%酒店%' or contact like '%百货%'
                or contact like '%用车服务%' or contact like '%高铁%' or contact like '%票%' or contact like '%硬座%'
                or contact like '%软卧%' or contact like '%卧铺%' or contact like '%航班%' then 1 else 0 end) single_mention_num,
        '企微聊天' as channel, 3 as channel_id
    from dw.kn1_officeweixin_sender_cust_content
    where dt between '20230726' and '20230809'
        and cust_id in (select * from tmp_cust_id_20230809)
    group by cust_id, to_date(msg_time)
    union all
    -- 在线客服沟通明细
    select
        cust_id,
        to_date(create_start_time) create_time,
        null as comm_duration,                  -- 聊天时长：null
        count(1) comm_num,                      -- 发送消息数
        sum(case when content like '%客人发送消息%' then 1 else 0 end) active_comm_num,    -- 用户主动发送消息数
        sum(case when content like '%商旅%' or content like '%跟团%' or content like '%自驾%' or content like '%自助%'
                or content like '%目的地服务%' or content like '%签证%' or content like '%团队%' or content like '%定制%'
                or content like '%游轮%' or content like '%旅拍%' or content like '%游%' or content like '%团%' then 1 else 0 end) vac_mention_num,
        sum(case when content like '%火车票%' or content like '%机票%' or content like '%酒店%' or content like '%百货%'
                or content like '%用车服务%' or content like '%高铁%' or content like '%票%' or content like '%硬座%'
                or content like '%软卧%' or content like '%卧铺%' or content like '%航班%' then 1 else 0 end) single_mention_num,
        '在线客服' as channel, 4 as channel_id
    from dw.kn1_autotask_user_acsessed_chat
    where dt between '20230726' and '20230809'
        and cust_id in (select * from tmp_cust_id_20230809)
    group by cust_id, to_date(create_start_time)
) tmp;
```
``` sql
-- 计算用户的行为特征——浏览特征 + 沟通特征
drop table if exists tmp_cust_behavior_info_20230810;
create table tmp_cust_behavior_info_20230810 as
select
    t.cust_id,                                                                  -- 会员ID
    nvl(datediff('2023-08-10', least(a.first_operate_date, b.first_create_time)), 0) decision_time,    -- 决策时间
    a.first_operate_date,                                                       -- 历史15天内第一次浏览时间 
    nvl(a.browsing_days, 0) browsing_days,                                      -- 历史浏览天数
    nvl(a.pv_daily_avg, 0) pv_daily_avg,                                        -- 每日平均pv
    nvl(a.browsing_time, 0) as browsing_time,                                   -- 用户总浏览时间
    nvl(a.browsing_time_daily_avg, 0) browsing_time_daily_avg,                  -- 每日平均浏览时间
    nvl(a.browsing_products_cnt, 0) browsing_products_cnt,                      -- 历史浏览产品量
    nvl(a.browsing_products_cnt_daily_avg, 0) browsing_products_cnt_daily_avg,  -- 每日平均浏览产品量
    a.lowest_price_avg,                                                         -- 历史浏览产品（最低价）平均价
    nvl(a.browsing_vac_prd_cnt, 0) browsing_vac_prd_cnt,                        -- 历史浏览的度假产品数量
    nvl(a.browsing_single_prd_cnt, 0) browsing_single_prd_cnt,                  -- 历史浏览的单资源产品数量
    nvl(a.collect_coupons_cnt, 0) collect_coupons_cnt,                          -- 优惠券领券个数
    b.first_create_time,                                                        -- 历史15天内第一次沟通时间
    nvl(b.comm_days, 0) comm_days,                                              -- 历史沟通天数
    nvl(b.call_pct, 0) call_pct,                                                -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    nvl(b.robot_pct, 0) robot_pct,                                              -- 智能外呼占比
    nvl(b.officewx_pct, 0) officewx_pct,                                        -- 企微聊天占比
    nvl(b.channels_cnt, 0) as channels_cnt,                                     -- 历史沟通渠道数（count(distinct channel)）
    nvl(b.calls_freq, 0) calls_freq,                                            -- 通话频数：电话+智能外呼
    nvl(b.active_calls_freq, 0) active_calls_freq,                              -- 用户主动通话频数：电话+智能外呼
    nvl(b.chats_freq_daily_avg, 0) chats_freq_daily_avg,                        -- 每日平均聊天频数：企微聊天+在线客服
    nvl(b.active_chats_freq, 0) active_chats_freq,                              -- 用户主动聊天频数：企微聊天+在线客服
    nvl(b.active_chats_pct, 0) active_chats_pct,                                -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    nvl(b.vac_mention_num, 0) vac_mention_num,                                  -- 沟通过程中度假产品的总提及次数
    nvl(b.single_mention_num, 0) single_mention_num                             -- 沟通过程中度假产品的总提及次数
from tmp_cust_id_20230809 t
left join (
    select  
        cust_id,
        min(operate_date) first_operate_date,
        count(distinct operate_date) browsing_days,
        round(count(visitor_trace) / count(distinct operate_date), 4) pv_daily_avg,
        sum(residence_time) browsing_time,
        round(sum(residence_time) / count(distinct operate_date), 4) browsing_time_daily_avg,
        count(distinct product_id) browsing_products_cnt,
        round(count(distinct product_id) / count(distinct operate_date), 4) browsing_products_cnt_daily_avg,
        round(avg(lowest_price), 4) lowest_price_avg,
        sum(case when producttype_class_name like '度假产品' then 1 else 0 end) browsing_vac_prd_cnt,
        sum(case when producttype_class_name like '单资源产品' then 1 else 0 end) browsing_single_prd_cnt,
        count(distinct case when collect_coupons_status = 1 then operate_date else null end) collect_coupons_cnt
    from tmp_cust_browsing_info_20230809
    group by cust_id
) a on a.cust_id = t.cust_id
left join (
    select  
        cust_id,
        min(create_time) first_create_time,
        count(distinct create_time) comm_days,
        round(sum(if(channel_id=1,1,0)) / count(channel_id), 4) call_pct,
        round(sum(if(channel_id=2,1,0)) / count(channel_id), 4) robot_pct,
        round(sum(if(channel_id=3,1,0)) / count(channel_id), 4) officewx_pct,
        count(distinct channel_id) channels_cnt,
        sum(if(channel_id in (1,2), comm_num, 0)) calls_freq,
        sum(if(channel_id in (1,2), active_comm_num, 0)) active_calls_freq,
        round(sum(if(channel_id in (3,4), comm_num, 0)) / count(distinct if(channel_id in (3,4), create_time, null)), 4) chats_freq_daily_avg,
        sum(if(channel_id in (3,4), active_comm_num, 0)) active_chats_freq,
        round(sum(if(channel_id in (3,4), active_comm_num, 0))/sum(if(channel_id in (3,4), comm_num, 0)), 4) active_chats_pct,
        sum(vac_mention_num) vac_mention_num,
        sum(single_mention_num) single_mention_num
    from tmp_cust_communication_info_20230809
    group by cust_id
) b on b.cust_id = t.cust_id;
```
``` sql
-- 汇总以上表
drop table if exists tmp_order_cust_feature_behavior_info_20230810;
create table tmp_order_cust_feature_behavior_info_20230810 as
select
    t1.cust_id,                                                 -- 会员ID
    t1.cust_level,                                              -- 会员星级
    t1.cust_type,                                               -- 会员类型：1 老带新新会员,2 新会员(新客户),3 老会员(新客户),4 老会员老客户,-1 其他
    t1.sex,                                                     -- 用户性别
    t1.age_group,                                               -- 用户年龄段
    t1.access_channel_cnt,                                      -- 可触达渠道的个数
    nvl(t2.executed_orders_count, 0) executed_orders_count,     -- 历史15天内成交订单数
    nvl(t2.complaint_orders_count, 0) complaint_orders_count,   -- 历史15天内投诉订单数
    t3.decision_time,                                           -- 决策时间
    t3.browsing_days,                                           -- 历史浏览天数
    t3.pv_daily_avg,                                            -- 每日平均pv
    t3.browsing_time,                                           -- 用户总浏览时间
    t3.browsing_time_daily_avg,                                 -- 每日平均浏览时间
    t3.browsing_products_cnt,                                   -- 历史浏览产品量
    t3.browsing_products_cnt_daily_avg,                         -- 每日平均浏览产品量
    t3.lowest_price_avg,                                        -- 历史浏览产品（最低价）平均价
    t3.browsing_vac_prd_cnt,                                    -- 历史浏览的度假产品数量
    t3.browsing_single_prd_cnt,                                 -- 历史浏览的单资源产品数量
    t3.collect_coupons_cnt,                                     -- 优惠券领券个数
    t3.comm_days,                                               -- 历史沟通天数
    t3.call_pct,                                                -- 电话呼入呼出占比（电话呼入呼出/总沟通次数）
    t3.robot_pct,                                               -- 智能外呼占比
    t3.officewx_pct,                                            -- 企微聊天占比
    t3.channels_cnt,                                            -- 历史沟通渠道数（count(distinct channel)）
    t3.calls_freq,                                              -- 通话频数：电话+智能外呼
    t3.active_calls_freq,                                       -- 用户主动通话频数：电话+智能外呼
    t3.chats_freq_daily_avg,                                    -- 每日平均聊天频数：企微聊天+在线客服
    t3.active_chats_freq,                                       -- 用户主动聊天频数：企微聊天+在线客服
    t3.active_chats_pct,                                        -- 用户主动聊天占比 = 用户主动聊天频数 / 聊天频数
    t3.vac_mention_num,                                         -- 沟通过程中度假产品的总提及次数
    t3.single_mention_num                                       -- 沟通过程中度假产品的总提及次数
from tmp_order_cust_feature t1
left join tmp_cust_historical_order t2
on t2.cust_id = t1.cust_id
left join tmp_cust_behavior_info_20230810 t3
on t3.cust_id = t1.cust_id
where t1.is_inside_staff = 0 and t1.is_distribution = 0 and t1.is_scalper = 0;   -- 剔除内部会员/分销/黄牛
```
&emsp;&emsp;取出2023-08-10用户订单实际成交情况。
``` sql
-- 数据验证：取出20230810实际数据
drop table if exists tmp_order_20230810;
create table tmp_order_20230810 as
select 
    a.cust_id,                                                     -- 会员ID
    case when max(case when b.executed_flag = 1 and c.producttype_class_name like '度假产品' then 1 else 0 end) = 1 
        then 1 else 0 end cust_executed_flag,
    sum(case when b.executed_flag = 1 and c.producttype_class_name like '度假产品' then 1 else 0 end) executed_count
from (
    select cust_id from tmp_order_cust_feature_behavior_info_20230810
) a
left join (
    select 
        cust_id, order_id, route_id, book_city,
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, 1, 0) executed_flag
    from dw.kn2_ord_order_detail_all
    where dt = '20230810' and create_time >= '2023-08-10'   -- 统计2023-08-10的订单
        and valid_flag=1 and is_sub = 0 and distribution_flag in (0, 3, 4, 5)
) b on b.cust_id = a.cust_id
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) c on c.route_id = b.route_id and c.book_city = b.book_city
group by a.cust_id;
```

&emsp;&emsp;基于上述建立的随机森林模型，对2023-08-10下单用户的成交情况进行预测，预测结果如下。
``` python
order_1 = pd.read_csv("./tmp_order_cust_feature_behavior_info_20230810.csv", encoding='utf-16',sep='\t')
data_1 = order_1.copy()
data_1['sex'] = data_1['sex'].map({'男': 1, '女': 0})
data_1['age_group'] = data_1['age_group'].map({'0~15岁': 1, '16~25岁': 2, '26~35岁': 3, '36~45岁': 4, '大于46岁': 5})
data_1.iloc[:, 1:5] = data_1.iloc[:, 1:5].astype('category')
data_1

# 缺失值填充
data_1['access_channel_cnt'] = data_1['access_channel_cnt'].fillna(0) 
data_1['lowest_price_avg'] = data_1['lowest_price_avg'].fillna(data_1['lowest_price_avg'].mean()) 

# 数据标准化
from sklearn.preprocessing import StandardScaler
data_std_1 = data_1.copy()
scaler = StandardScaler()
data_std_1.iloc[:, 5:] = scaler.fit_transform(data_1.iloc[:, 5:])

# 对类别变量进行独热编码
encoded_data_1 = pd.get_dummies(data_1.iloc[:, 1:5], prefix=data_1.iloc[:, 1:5].columns, drop_first=True).astype(int)

# 将编码后的数据与原始数据合并
final_data_1 = pd.concat([data_std_1.iloc[:,0], encoded_data_1, data_std_1.iloc[:, 5:]], axis=1)
final_data_1.info()

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import joblib

# 加载模型
loaded_rf_classifier = joblib.load('./rf_classifier_model.joblib')
# 使用加载的模型进行预测
X_test_1 = final_data_1.iloc[:, 1:]
proba_predictions = loaded_rf_classifier.predict_proba(X_test_1)

# 获取类别为1的概率预测值
class_1_probabilities = proba_predictions[:, 1]
# 根据条件生成 y_pred_loaded
y_pred_loaded = [1 if prob >= 0.5 else 0 for prob in class_1_probabilities]
y_pred_series = pd.Series(y_pred_loaded, name='executed_flag_pred')
result = pd.concat([final_data_1['cust_id'].reset_index(drop=True), y_pred_series], axis=1)

# 获取2023.08.10当天实际成交数据量
test = pd.read_csv('./tmp_order_20230810.csv', encoding='utf-16',sep='\t')
merged_df= pd.merge(test, result, on='cust_id')

# 计算模型评价指标
y_test = merged_df['cust_executed_flag']
y_pred_loaded = merged_df['executed_flag_pred']
print("AUC (Loaded Model):", round(roc_auc_score(y_test, y_pred_loaded), 4))
print(classification_report(y_test, y_pred_loaded))
print("混淆矩阵 (Loaded Model):", confusion_matrix(y_test, y_pred_loaded))
```

<style type="text/css">
.tg  {border-collapse:collapse;border-spacing:0;}
.tg td{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  overflow:hidden;padding:10px 5px;word-break:normal;}
.tg th{border-color:black;border-style:solid;border-width:1px;font-family:Arial, sans-serif;font-size:14px;
  font-weight:normal;overflow:hidden;padding:10px 5px;word-break:normal;}
.tg .tg-baqh{text-align:center;vertical-align:top}
.tg .tg-t4dz{background-color:#B0D4CC;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-amwm{font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-v658{background-color:#72B7EF;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-zm63{background-color:#B7DCFB;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-50tr{background-color:#D8ECE7;font-weight:bold;text-align:center;vertical-align:top}
.tg .tg-0lax{text-align:left;vertical-align:top}
</style>
<table class="tg" style="undefined;table-layout: fixed; width: 268px">
<colgroup>
<col style="width: 66px">
<col style="width: 65px">
<col style="width: 69px">
<col style="width: 68px">
</colgroup>
<thead>
  <tr>
    <th class="tg-amwm" colspan="2" rowspan="2"><span style="font-weight:bold">混淆矩阵</span></th>
    <th class="tg-v658" colspan="2"><span style="font-weight:bold">预测值</span></th>
  </tr>
  <tr>
    <th class="tg-zm63"><span style="font-weight:bold">0</span></th>
    <th class="tg-zm63"><span style="font-weight:bold">1</span></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td class="tg-t4dz" rowspan="2"><span style="font-weight:bold">真实值</span></td>
    <td class="tg-50tr"><span style="font-weight:bold">0</span></td>
    <td class="tg-baqh">103988</td>
    <td class="tg-baqh">4066</td>
  </tr>
  <tr>
    <td class="tg-50tr"><span style="font-weight:bold">1</span></td>
    <td class="tg-baqh">267</td>
    <td class="tg-baqh">71</td>
  </tr>
</tbody>
</table>

<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309131618614.png" alt="模型测试结果" width="500" />
  <p>模型测试结果</p>
</div> 

&emsp;&emsp;利用训练好的随机森林模型对2023-08-10用户下单情况进行预测，AUC值为0.5862，预测准确率为96%，精确率为2%，召回率为21%。正样本量为338，负样本量为108054，样本分布极不平衡，导致了正样本的精准率较低。
&emsp;&emsp;模型有待进一步改进，后续考虑改进方向：
- 调整随机森林模型预测阈值：模型预测默认阈值为0.5，由于正负样本失衡，根据实际情况考虑降低正负样本的预测阈值；
- 调整预测标签：目前是对用户订单是否成交进行预测，包含了单资源产品和度假产品，后续调整为对度假产品的成交与否进行预测。

<br>

