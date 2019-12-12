import MonitorApi as monAPI
import json

id = '28566032711'
authToken = ""
#
monitor = monAPI.monitor_api(id, authToken)
# data = monitor.get_monitor_posts_request("2019-01-01", "2019-05-01", fullContents=True, extendLimit=True)

# print(monitor.monitorId)
# print(monitor.authenticationToken)

# df = monitor.json_2_pandas_posts(data)
# df.to_csv('kind2.csv')

# data = monitor.get_monitor_results_request("2015-06-01", "2020-06-02")
# df = monitor.json_2_pandas_results(data)
# df.to_csv('monitorResultsTest.csv')

# data = monitor.get_monitor_volume_request("2019-06-01", "2019-06-02", groupBy='HOURLY')
# df = monitor.json_2_pandas_volume(data)
# df.to_csv('monitorVolumeTest.csv')


# data = monitor.get_monitor_volume_by_dt_request("2019-05-01", "2019-06-02", aggregatedbyday='true')
# df = monitor.json_2_pandas_volume_by_dt(data)
# df.to_csv('monitorVolumeByDTTest.csv')

# data = monitor.get_monitor_posts_request("2008-05-01", "2019-08-30", fullContents=True, extendLimit=True)
# df = monitor.json_2_pandas_posts(data)
# df.to_csv('kindukbreakfastcompetitor.csv')

# data = monitor.get_monitor_age_request("2008-05-01", "2019-08-30")
# df = monitor.json_2_pandas_age(data)
# df.to_csv('kindukbre\akfastAGE.csv')

# data = monitor.get_twitter_metrics('2019-08-01','2019-09-27')
# x = monitor.json_2_pandas_twitter_metrics(data)

# data = monitor.get_twitter_sentposts('2016-08-01','2019-10-07')
# x = monitor.json_2_pandas_sentposts(data)
# print(x.head(n=50))

# data = monitor.get_twitter_followers('2016-08-01','2019-10-07')
# x = monitor.json_2_pandas_twitter_followers(data)
# print(x.head(n=50))

data = monitor.get_facebook_admin_posts('2016-08-01','2019-10-07')
print(data)
x = monitor.json_2_pandas_facebook_admin_posts(data)
print(x.head(n=50))


data = monitor.get_facebook_page_likes('2016-08-01','2019-10-07')
print(data)
x = monitor.json_2_pandas_facebook_page_likes(data)
print(x.head(n=50))


data = monitor.get_facebook_total_activity('2016-08-01','2019-10-07')
print(data)
x = monitor.json_2_pandas_facebook_total_activity(data)
print(x.head(n=50))

# data = monitor.get_instagram_followers('2016-08-01','2019-10-07')
# x = monitor.json_2_pandas_instagram_followers(data)
# print(x.head(n=50))

# data = monitor.get_instagram_sent_media('2016-08-01','2020-10-20')
# x = monitor.json_2_pandas_instagram_sent_media(data)
# print(x.head(n=50))

# data = monitor.get_instagram_total_activity('2016-08-01','2020-10-20')
# x = monitor.json_2_pandas_instagram_total_activity(data)
# print(x.head(n=50))