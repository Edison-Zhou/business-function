use ai;
--电视猫的底层基本数据计算，包括播放、点击、收藏、点赞的数据，按照天进行计算，
--useridoraccountid用来标识是userid还是accountid
--optime是记录用户在该节目上的最晚操作（包括播放、点击等）时间
--之所以要join  dim_medusa_program表，是存在原始日志中contenttype不准的问题，所以要去媒资中获取contenttype

--播放日志的统计
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='play',useridoraccountid = 1)
select userid2long(userid) as userid,max(a.datetime)as optime,
  b.sid,b.content_type,
  if(sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  )<=5,sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  ),5)as  play_ratio
from
(
  SELECT user_id as userid,concat_ws(" ",dim_date,dim_time) as datetime ,duration, if(episode_program_sk is not null,episode_program_sk,program_sk) as program_sk
  from dw_facts.fact_medusa_play
  where day_p = '${startDate}'
  and play_type = 'positive'
  and user_id is not null
  and account_id is null
  and duration > 0
  and end_event != 'noEnd'
  GROUP BY user_id,concat_ws(" ",dim_date,dim_time),duration,episode_program_sk,program_sk
) a
 INNER join
 (
    SELECT parent_sid as sid,duration,content_type,program_sk
    from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL
  ) b
 on a.program_sk = b.program_sk
 GROUP BY a.userid,b.sid,b.content_type
 ;

--登陆账户后的行为--播放
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = '${startDate}',score_source='play',useridoraccountid = 0)
select a.accountid,max(a.datetime)as optime,
  b.sid,b.content_type,
  if(sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  )<=5,sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  ),5)as  play_ratio
from
(
  SELECT account_id  as accountid,concat_ws(" ",dim_date,dim_time) as datetime ,duration, if(episode_program_sk is not null,episode_program_sk,program_sk) as program_sk
  from dw_facts.fact_medusa_play
  where day_p = '${startDate}'
  and play_type = 'positive'
  and account_id is not null
  and duration > 0
  and end_event != 'noEnd'
  GROUP BY account_id,concat_ws(" ",dim_date,dim_time),duration,episode_program_sk,program_sk
) a
 INNER join
 (
    SELECT parent_sid as sid,duration,content_type,program_sk
    from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL
  ) b
  on a.program_sk = b.program_sk
 GROUP BY a.accountid,b.sid,b.content_type
  ;

