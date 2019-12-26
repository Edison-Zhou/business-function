use ai;

--本表主要是记录用户的曝光行为数据，在算法使用过程中，可以基于近期的曝光行为对待推荐集进行剔除处理

--首页今日推荐模块
--accessarea = 'recommendation'   这个表示今日推荐的条件
insert overwrite table ai.ai_base_behavior_display
partition(product_line='moretv',biz='portalRecommend', key_time = '${startDate}')
select userid,sid
from
(
    SELECT recommendlist_tmp.locationindex,recommendlist_tmp.contentId as sid,
    (case when (accountid is not null) and  accountid > 0 then accountid
          else userid2long(userid) end) as userid
    FROM ods_view.log_medusa_main3x_medusa_launcher_area_quit
    LATERAL VIEW explode(recommendlist) myTable1 AS recommendlist_tmp
    where key_day = '${startDate}' and accessarea = 'recommendation'
)a
where locationindex > 1 and sid !='' and sid is not null and length(sid)<= 15 and userid > 0
group by userid,sid;


--首页今日推荐(焦点停留在第三行)
--第三行的推荐位角标从9-13
insert overwrite table ai.ai_base_behavior_display
partition(product_line='moretv',biz='portalRecommend_ex', key_time = '${startDate}')
select userid,sid
from
(
    SELECT recommendlist_tmp.locationindex,recommendlist_tmp.contentId as sid,
    (case when (accountid is not null) and  accountid > 0 then accountid
          else userid2long(userid) end) as userid
    FROM ods_view.log_medusa_main3x_medusa_launcher_area_quit
    LATERAL VIEW explode(recommendlist) myTable1 AS recommendlist_tmp
    where key_day = '${startDate}' and accessarea = 'recommendation'
    and (array_contains(focuslist,'9') or array_contains(focuslist,'10') or array_contains(focuslist,'11')
                  or array_contains(focuslist,'12')
                  or array_contains(focuslist,'13')  )
)a
where locationindex > 1 and sid !='' and sid is not null and length(sid)<= 15 and userid > 0
group by userid,sid;

--首页会员看看
--accessarea = 'memberArea'  表示会员看看模块
insert overwrite table ai.ai_base_behavior_display
partition(product_line='moretv',biz='memberArea', key_time = '${startDate}')
select userid,sid
from
(
    SELECT recommendlist_tmp.locationindex,recommendlist_tmp.contentId as sid,
    (case when (accountid is not null) and  accountid > 0 then accountid
          else userid2long(userid) end) as userid
    FROM ods_view.log_medusa_main3x_medusa_launcher_area_quit
    LATERAL VIEW explode(recommendlist) myTable1 AS recommendlist_tmp
    where key_day = '${startDate}' and accessarea = 'memberArea'
)a
where locationindex > 1 and sid !='' and sid is not null and length(sid)<= 15 and userid > 0
group by userid,sid;
