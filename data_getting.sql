--------------------------------------------------------------------建模数据--------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
-- 订单信息（包括已成交和未成交订单）
drop table if exists tmp_order_202307;
create table tmp_order_202307 as
select 
    a.cust_id,                                                     -- 会员编号
    a.order_id,                                                    -- 订单号
    a.create_time,                                                 -- 下单时间
    a.executed_date,                                               -- 订单成交日期
    case when a.executed_flag = 1 and b.producttype_class_name like '度假产品' then 1
        when a.executed_flag = 1 and b.producttype_class_name like '单资源产品' then 2
        else 0 end executed_flag,                                  -- 订单成交标识 0：否，1：度假产品成交，2：单资源产品成交
    nvl(a.executed_date, cast(a.create_time as date)) order_date,  -- 订单日期：用下单时间补全成交日期
    a.route_id,                                                    -- 线路号
    b.producttype_class_name,                                      -- 产品大类
    a.star_level,                                                  -- 订单星级
    a.complaint_flag,                                              -- 是否有投诉 1是 0否
    a.complaint_time                                               -- 投诉时间
from (
    select 
        cust_id, order_id, create_time,
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, sign_date, null) executed_date,
        if(cancel_flag=0 and sign_flag=1 and cancel_sign_flag=0, 1, 0) executed_flag,
        route_id, book_city,
        star_level, complaint_flag, complaint_time
    from dw.kn2_ord_order_detail_all
    where dt = '20230731' and create_time >= '2023-07-01'   -- 统计7月份的订单
        and valid_flag=1                                    -- 有效标记
        and is_sub = 0                                      -- 剔除子订单
        and distribution_flag in (0, 3, 4, 5)               -- 剔除分销
        and cust_id in (select distinct cust_id from dw.kn1_usr_cust_attribute) -- 剔除已注销用户
) a
left join (
    select 
        distinct route_id, book_city,
        case when one_producttype_name like '门票' then '单资源产品' else producttype_class_name end producttype_class_name
    from dw.kn2_dim_route_product_sale
) b on a.route_id = b.route_id and a.book_city = b.book_city;


-------------------------------------------------------------------------------------------------------------------------------------
-- 创建临时表存储去重后的cust_id，简化后续查询
drop table if exists tmp_order_cust_id;
create table tmp_order_cust_id as
select distinct cust_id from tmp_order_202307;


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------测试数据--------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------
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


-------------------------------------------------------------------------------------------------------------------------------------

