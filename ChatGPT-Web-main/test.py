import redis2 as redis
import pymysql
import time
import random

if __name__ == '__main__':
    redis_cli = redis.StrictRedis(host='35.74.232.91', port=6379, db=0,
                                  password='F!f!y!@#iy#@**W78&')  # redis数据库连接池2.10.6

    conn = pymysql.connect(host='35.74.232.91', port=3306, user='amisadmin', passwd='LMizbFde4zwip7Lw', db='amisadmin',
                           charset='utf8mb4')
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    # redis_up()
    while 1:
        # 判断urls长度
        urls_len = redis_cli.llen("twitter01_start_urls")
        if urls_len > 0:
            time.sleep(1)
            continue

        sql = 'select twitter_name, twitter_id, user_name from `fa_up` where types = 1'
        conn.ping(reconnect=True)  # 提前ping一下
        cursor.execute(sql)
        res_list = cursor.fetchall()
        value_list = []
        for res_dict in res_list[:]:
            value = res_dict.get("twitter_name") + "," + res_dict.get("twitter_id") + "," + res_dict.get("user_name")
            value_list.append(value)
        random.shuffle(value_list)
        redis_cli.lpush("twitter01_start_urls", *value_list)
        time.sleep(4)

    cursor.close()
    conn.close()