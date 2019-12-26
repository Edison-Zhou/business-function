use ai;
--用户行为原始聚合
drop table if exists ai_base_behavior_raw;
create external table if not exists `ai_base_behavior_raw`(
  `userid` bigint COMMENT '用户账户',
  `optime` string COMMENT '操作日期',
  `sid_or_subject_code` string COMMENT '节目或专题ID',
  `content_type` string COMMENT '节目类别',
  `score` double COMMENT '评分值')
PARTITIONED by (
  `product_line` string,`partition_tag` string,`score_source` string,`useridoraccountid` int)
STORED AS PARQUET
LOCATION
  '/ai/etl/ai_base_behavior_raw';

--用户行为算法求解后的数据表
drop table if exists ai_base_behavior;
create external table if not exists `ai_base_behavior`(
  `userid` bigint COMMENT '用户账户',
  `latest_optime` string COMMENT '最新更新日期',
  `sid_or_subject_code` string COMMENT '节目或专题ID',
  `score` double COMMENT '评分值')
PARTITIONED BY (
  `product_line` string,`partition_tag` string,`content_type` string)
STORED AS PARQUET
LOCATION
 '/ai/etl/ai_base_behavior';

--用户行为中的曝光日志
drop table if exists ai_base_behavior_display;
create external table if not exists  `ai_base_behavior_display`(
  `userid` bigint COMMENT '用户账户',
  `sid` string COMMENT '节目或专题ID')
PARTITIONED BY (
  `product_line` string,`biz` string,`key_time` string)
STORED AS PARQUET
LOCATION
  '/ai/etl/ai_base_behavior_display';

--用户行为原始聚合-剧集类数据
drop table if exists ai_base_behavior_raw_episode;
create external table if not exists  `ai_base_behavior_raw_episode`(
  `userid` bigint COMMENT '用户账户',
  `sid` string COMMENT '节目或专题ID',
  `optime` string COMMENT '操作日期',
  `content_type` string COMMENT '节目类别',
  `episodesid` string COMMENT '剧集ID',
  `episode_index` string COMMENT '剧集序号',
  `score` double COMMENT '评分值')
PARTITIONED BY (
  `product_line` string, `score_source` string,`partition_tag` string)
STORED AS PARQUET
LOCATION
  '/ai/etl/ai_base_behavior_raw_episode';