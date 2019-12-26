use ai;
--电视猫产品线，主要用于追剧模型，记录用户在观看过程的中对于剧集类节目的观看行为，会统计当前看的节目属于剧集类的第几集
--为了业务上的需要，也把电影一起进行汇总

--播放日志的统计
insert overwrite table ai.ai_base_behavior_raw_episode
partition(product_line='moretv', partition_tag = '${startDate}',score_source='play')
select a.userid,b.parent_sid,max(a.datetime)as optime,b.content_type,b.sid,b.episode_index,
if(sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  )<=10,sum(
  case when (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) > 1 then 1
  else (a.duration / ( RepairDuration( cast(b.duration as int),b.content_type)*60) ) end
  ),10)as  play_ratio
 from
(
select (case when (account_id is not null) and  account_id > 0 then account_id else userid2long(user_id)end) as userid,
concat_ws(" ",dim_date,dim_time) as datetime,
duration,if(episode_program_sk is not null,episode_program_sk,program_sk) as program_sk
from dw_facts.fact_medusa_play
where day_p = '${startDate}' and duration > 0 and end_event != 'noEnd'
GROUP BY account_id,user_id,concat_ws(" ",dim_date,dim_time),duration,episode_program_sk,program_sk
) a
inner join
(
SELECT parent_sid,sid,duration,content_type,episode_index,program_sk
 from dw_dimensions.dim_medusa_program where dim_invalid_time is NULL and video_type != 1
) b
on a.program_sk = b.program_sk
group by userid,parent_sid,sid,episode_index,content_type
;

