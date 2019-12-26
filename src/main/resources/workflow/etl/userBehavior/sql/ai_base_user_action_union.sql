use ai;
--用户登陆时的合并行为
ALTER TABLE  ai.dw_base_user_action_union
ADD if not exists PARTITION (product_line='moretv',key_time =${startDate} )
location '/ai/dw/moretv/base/UserActionUnion/${startDate}';