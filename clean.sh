#!/bin/bash

# 获取当前用户名
USER=$(whoami)

# 查找并终止 Python 爬虫进程
echo "Killing Python crawler process..."
ps -u "$USER" | grep "python3" | awk '{print $1}' | xargs -r kill -9
echo "All Python processes killed."

# 删除日志和数据文件
echo "Removing logs and output files..."
rm -f crawler_analytics.txt longest_page.txt output.log frontier.shelve

# 删除 Logs 目录中的所有日志文件
rm -f Logs/*

# 显示完成消息
echo "Cleanup completed."
