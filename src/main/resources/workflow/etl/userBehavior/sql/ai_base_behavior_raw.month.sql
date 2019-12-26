use ai;
--电视猫产品的月度聚合数据，为了后续总体计算时候更快
--月度聚合数据
insert overwrite table ai.ai_base_behavior_raw
partition(product_line='moretv', partition_tag = 'M${currentMonth}',score_source,useridoraccountid)
select userid,max(optime),sid_or_subject_code,content_type,sum(score),score_source,useridoraccountid
from ai.ai_base_behavior_raw
where partition_tag >= '${currentMonth}01' and partition_tag <= '${currentMonth}31'
and product_line='moretv'
group by userid,sid_or_subject_code,content_type,score_source,useridoraccountid;

