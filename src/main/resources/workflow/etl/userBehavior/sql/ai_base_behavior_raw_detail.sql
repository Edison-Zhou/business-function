use ai;
--电视猫的底层基本数据计算，包括播放、点击、收藏、点赞的数据，按照天进行计算，
--useridoraccountid用来标识是userid还是accountid
--optime是记录用户在该节目上的最晚操作（包括播放、点击等）时间
--之所以要join  dim_medusa_program表，是存在原始日志中contenttype不准的问题，所以要去媒资中获取contenttype


--点击日志的统计
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='detail',useridoraccountid = 1)
select userid2long(userid) as userid,max(a.datetime) as optime,b.sid,b.content_type,1 as score
from
(
  SELECT userid,videosid,datetime
  from ods_view.log_medusa_main3x_detail
  where key_day = '${startDate}'
  and videosid is not null
  and userid is not null
  and accountid is null
) a
 INNER join
(
    SELECT sid,content_type
    from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL
) b
on a.videosid = b.sid
GROUP BY a.userid,b.sid,b.content_type
;

--点击日志的统计会员登陆后的数据
--点击次数作为观察用户喜好的重要目标
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='detail',useridoraccountid = 0)
select accountid,max(a.datetime) as optime,b.sid,b.content_type,1 as score
from
(
  SELECT accountid,videosid,datetime
  from ods_view.log_medusa_main3x_detail
  where key_day = '${startDate}'
  and videosid is not null
  and accountid is not null
) a
 INNER join
(
    SELECT sid,content_type
    from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL
) b
on a.videosid = b.sid
GROUP BY a.accountid,b.sid,b.content_type
;