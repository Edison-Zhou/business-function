### azkaban依赖包更新步骤
1. 通过maven的package命令生成lib目录，压缩成lib.zip
2. 通过fliezilla上传lib到ftp上
3. 登录到管理机上 24：bigdata-extsvr-sys-manager
4. 选用spark用户
5. 切换目录  
cd /data/tools/ansible/modules/azkaban/playbook
6. 下载ftp上的lib.zip文件
ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/ai/doraemon/;python /data/tscripts/scripts/ftp.py -s get -f lib.zip"
7. 批量删除原lib目录
ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/ai/doraemon/;rm -rf lib"
8. 解压
ansible all -i azkaban_server.host -mshell -a "cd /data/apps/azkaban/ai/doraemon/;unzip lib.zip"