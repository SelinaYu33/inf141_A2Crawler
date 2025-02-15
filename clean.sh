#!/bin/bash

USER=$(whoami)

echo "Killing Python crawler process..."
ps -u "$USER" | grep "python3" | awk '{print $1}' | xargs -r kill -9
echo "All Python processes killed."

echo "Removing logs and output files..."
rm -f crawler_analytics.txt longest_page.txt output.log frontier.shelve

rm -f Logs/*

echo "Cleanup completed."
