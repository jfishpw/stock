#!/bin/sh

export PYTHONIOENCODING=utf-8
export LANG=zh_CN.UTF-8
export PYTHONPATH=/data/InStock
export LC_CTYPE=zh_CN.UTF-8

# 环境变量输出
printenv | grep -v "no_proxy" >> /etc/environment

# 确保crontab配置正确
echo "SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
# min hour day month weekday command
*/30 9,10,11,13,14,15 * * 1-5 /bin/run-parts /etc/cron.hourly
30 17 * * 1-5 /bin/run-parts /etc/cron.workdayly
30 10 * * 3,6 /bin/run-parts /etc/cron.monthly
" > /var/spool/cron/crontabs/root

chmod 600 /var/spool/cron/crontabs/root

# 启动cron服务。在前台
/usr/sbin/cron -f
