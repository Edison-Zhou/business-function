use ai;
set mapreduce.map.java.opts=-Xmx6144m ;
set mapreduce.map.memory.mb=2048 ;
set mapreduce.reduce.java.opts=-=-Xmx6144m ;
set mapreduce.reduce.memory.mb=2048 ;

--V1版本的计算，主要是针对电视猫产品线，将播放、点击、收藏、点赞的评分数据进行聚合，为统一的算法底层数据基础平台提供支持
insert overwrite table ai.ai_base_behavior
partition(product_line='moretv', partition_tag = 'V1',content_type)
select userid,max(optime),if(b.sid is null,a.sid,b.virtual_sid) as sid,sum(score) as score,content_type
from
(
  select userid,sid_or_subject_code as sid ,content_type,optime,
  (case when score_source = 'play' then score  when score_source = 'collect' then score*0.5 else 0  end) as score
  from ai.ai_base_behavior_raw
  where partition_tag like 'M%' and score_source in('play','collect') and product_line='moretv'
) a
  left join
(
  select virtual_sid,sid from
  ods_view.db_snapshot_mysql_medusa_mtv_virtual_content_rel
  where key_day='latest' and status=1
) b
  on  a.sid =b.sid
group by userid,if(b.sid is null,a.sid,b.virtual_sid),content_type
;

--电视猫长视频播放记录统计，主要是播放历史记录，解决用户推荐结果的消除历史记录问题
insert overwrite table ai.ai_base_behavior
partition(product_line='moretv', partition_tag = 'longVideoHistory',content_type)
select userid,max(optime), if(b.sid is null,a.sid,b.virtual_sid) as sid ,sum(score) as score,content_type
from
(
  select userid,sid_or_subject_code as sid ,content_type,optime,score
  from ai.ai_base_behavior_raw
  where partition_tag like 'M%' and score_source = 'play'
  and content_type in('movie', 'tv', 'zongyi', 'comic', 'kids', 'jilu') and product_line='moretv'
) a
left join
(
  select virtual_sid,sid
  from ods_view.db_snapshot_mysql_medusa_mtv_virtual_content_rel
  where key_day='latest' and status=1
) b
on  a.sid =b.sid
group by userid,if(b.sid is null,a.sid,b.virtual_sid) ,content_type
;
