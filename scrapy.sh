#!/bin/sh
source ~/Scrapy/venv/bin/activate;
cd ~/Scrapy/zhaobiao/zhaobiao;
scrapy crawl zhaobiao -a keyword=$1
echo "$1"
