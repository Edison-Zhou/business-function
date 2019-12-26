use ai;
--电视猫的底层基本数据计算，包括播放、点击、收藏、点赞的数据，按照天进行计算，
--useridoraccountid用来标识是userid还是accountid
--optime是记录用户在该节目上的最晚操作（包括播放、点击等）时间
--之所以要join  dim_medusa_program表，是存在原始日志中contenttype不准的问题，所以要去媒资中获取contenttype

--收藏日志的统计
--collectclass = 'video' 表示只关心单节目
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='collect',useridoraccountid = 1)
select userid2long(a.userid) as userid ,max(a.datetime) as optime,a.sid,b.content_type,1 as score
from
  (
    select userid,datetime ,
    collectcontent as sid
    from ods_view.log_medusa_main3x_collect
    WHERE  key_day = '${startDate}'
    and collectclass = 'video'
    and collectcontent is not null
    and userid is not null
    and accountid is  null
  ) a
  INNER  JOIN
  (
    SELECT sid,content_type
    from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL and status =1
  ) b
  on a.sid = b.sid
  GROUP by a.sid ,a.userid,b.content_type
 ;


--收藏日志的统计--用户登陆后的账户
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='collect',useridoraccountid = 0)
select a.accountid,max(a.datetime) as optime,a.sid,b.content_type,1 as score
from
  (
    select accountid,datetime,
    collectcontent as sid
    from ods_view.log_medusa_main3x_collect
    WHERE  key_day = '${startDate}'
    and collectclass = 'video'
    and collectcontent is not null
    and userid is not null
    and accountid is not null
  ) a
  INNER  JOIN
  (
    SELECT sid,contenttype as content_type
    from ods_view.db_snapshot_mysql_medusa_mtv_program where key_day='latest' and key_hour='latest' and status =1
  ) b
  on a.sid = b.sid
  GROUP by a.sid ,a.accountid,b.content_type
  ;
