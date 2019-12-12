#
# Created by Denis Doci
#
# Copyright Mars Inc.
#
# For internal use only
#
import requests
import json
import pandas as pd
import numpy as np
import datetime
import itertools
import math
from datetime import date, timedelta

class monitor_api:

    monitorId = ''
    authenticationToken = ''

    def __init__(self, monitorId, authenticationToken):
        self.monitorId = monitorId
        self.authenticationToken = authenticationToken

    def create_empty_df(self, columns, dtypes, index=None):
        assert len(columns) == len(dtypes)
        df = pd.DataFrame(index=index)
        for c, d in zip(columns, dtypes):
            df[c] = pd.Series(dtype=d)
        return df
    # ***************************************************** ###
    # ***************************************************** ###
    # ***                                               *** ###
    # ***     Results: Volume, Sentiment & Categories   *** ###
    # *** The monitor results endpoint returns          *** ###
    # *** aggregate volume, sentiment, emotion and      *** ###
    # *** opinion category analysis for a given monitor *** ###
    # ***                                               *** ###
    # ***************************************************** ###
    # ***************************************************** ###
    # ***************************************************** ###

    def get_monitor_results_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/results"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        if "hideExcluded" in kwargs:
            querystring["hideExcluded"] = kwargs["hideExcluded"]
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)['results']
        return json_data

    def create_empty_result_df(self):
        df = self.create_empty_df(['startDate', 'endDate', 'creationDate',
                                   'numberOfDocuments', 'numberOfRelevantDocuments',
                                   'Basic_Positive_Proportion', 'Basic_Positive_Volume',
                                   'Basic_Negative_Proportion',
                                   'Basic_Negative_Volume', 'Basic_Neutral_Proportion', 'Basic_Neutral_Volume',
                                   'Emotion_Joy_Proportion', 'Emotion_Joy_Volume', 'Emotion_Sadness_Proportion',
                                   'Emotion_Sadness_Volume', 'Emotion_Anger_Proportion', 'Emotion_Anger_Volume',
                                   'Emotion_Disgust_Proportion', 'Emotion_Disgust_Volume',
                                   'Emotion_Surprise_Proportion',
                                   'Emotion_Surprise_Volume', 'Emotion_Fear_Proportion', 'Emotion_Fear_Volume',
                                   'Emotion_Neutral_Proportion', 'Emotion_Neutral_Volume'],
                                  dtypes=[np.str, np.str, np.str,
                                          np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float, np.float,
                                          np.float, np.float]
                                  )
        return df

    def results_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'startDate' in resultsRow.keys():
            row['startDate'] = resultsRow['startDate']
        else:
            row['startDate'] = ''
        if 'endDate' in resultsRow.keys():
            row['endDate'] = resultsRow['endDate']
        else:
            row['endDate'] = ''
        if 'numberOfDocuments' in resultsRow.keys():
            row['numberOfDocuments'] = resultsRow['numberOfDocuments']
        else:
            row['numberOfDocuments'] = ''
        if 'numberOfRelevantDocuments' in resultsRow.keys():
            row['numberOfRelevantDocuments'] = resultsRow['numberOfRelevantDocuments']
        else:
            row['numberOfRelevantDocuments'] = ''

        try:
            row['Basic_Positive_Proportion'] = resultsRow['categories'][2]['proportion']
            row['Basic_Positive_Volume'] = resultsRow['categories'][2]['volume']
            row['Basic_Negative_Proportion'] = resultsRow['categories'][0]['proportion']
            row['Basic_Negative_Volume'] = resultsRow['categories'][0]['volume']
            row['Basic_Neutral_Proportion'] = resultsRow['categories'][1]['proportion']
            row['Basic_Neutral_Volume'] = resultsRow['categories'][1]['volume']

        except:
            row['Basic_Positive_Proportion'] = ''
            row['Basic_Positive_Volume'] = ''
            row['Basic_Negative_Proportion'] = ''
            row['Basic_Negative_Volume'] = ''
            row['Basic_Neutral_Proportion'] = ''
            row['Basic_Neutral_Volume'] = ''

        try:
            row['Emotion_Joy_Proportion'] = resultsRow['emotions'][5]['proportion']
            row['Emotion_Joy_Volume'] = resultsRow['emotions'][5]['volume']
            row['Emotion_Sadness_Proportion'] = resultsRow['emotions'][2]['proportion']
            row['Emotion_Sadness_Volume'] = resultsRow['emotions'][2]['volume']
            row['Emotion_Anger_Proportion'] = resultsRow['emotions'][3]['proportion']
            row['Emotion_Anger_Volume'] = resultsRow['emotions'][3]['volume']
            row['Emotion_Disgust_Proportion'] = resultsRow['emotions'][4]['proportion']
            row['Emotion_Disgust_Volume'] = resultsRow['emotions'][4]['volume']
            row['Emotion_Surprise_Proportion'] = resultsRow['emotions'][1]['proportion']
            row['Emotion_Surprise_Volume'] = resultsRow['emotions'][1]['volume']
            row['Emotion_Fear_Proportion'] = resultsRow['emotions'][0]['proportion']
            row['Emotion_Fear_Volume'] = resultsRow['emotions'][0]['volume']
            row['Emotion_Neutral_Proportion'] = resultsRow['emotions'][6]['proportion']
            row['Emotion_Neutral_Volume'] = resultsRow['emotions'][6]['volume']

        except:
            row['Emotion_Joy_Proportion'] = ''
            row['Emotion_Joy_Volume'] = ''
            row['Emotion_Sadness_Proportion'] = ''
            row['Emotion_Sadness_Volume'] = ''
            row['Emotion_Anger_Proportion'] = ''
            row['Emotion_Anger_Volume'] = ''
            row['Emotion_Disgust_Proportion'] = ''
            row['Emotion_Disgust_Volume'] = ''
            row['Emotion_Surprise_Proportion'] = ''
            row['Emotion_Surprise_Volume'] = ''
            row['Emotion_Fear_Proportion'] = ''
            row['Emotion_Fear_Volume'] = ''
            row['Emotion_Neutral_Proportion'] = ''
            row['Emotion_Neutral_Volume'] = ''

        data.insert(0, row)

    def json_2_pandas_results(self, json_data):
        returnDf = self.create_empty_result_df()
        data = []
        for row in json_data:
            self.results_row_2_df_row(row, data)
        returnDf = pd.concat([pd.DataFrame(data), returnDf], ignore_index=True, sort=False)
        return returnDf

    # ****************************************************** ###
    # ****************************************************** ###
    # ***                    Posts                       *** ###
    # *** The posts endpoint returns post-level          *** ###
    # *** information (where available) and associated   *** ###
    # *** analysis (sentiment, emotion) for a given      *** ###
    # *** monitor.                                       *** ###
    # ****************************************************** ###
    # ****************************************************** ###
    def day_list(self, dates):
        return pd.date_range(dates[0], dates[1], freq='D').strftime("%Y-%m-%d").tolist()

    # recursive get posts INCLUSIVE: start_date EXCLUSIVE: end_date
    def recursive_pull(self, start_date, end_date, df_numPosts, **kwargs):
        # print(start_date)
        # print(end_date)
        numberPosts = df_numPosts.where(
            df_numPosts["startDate"] >= pd.to_datetime(datetime.datetime.strptime(start_date, '%Y-%m-%d')))
        numberPosts = numberPosts.where(
            df_numPosts["endDate"] <= pd.to_datetime(datetime.datetime.strptime(end_date, '%Y-%m-%d')))
        numberPosts = numberPosts['numberOfDocuments'].sum()
        # print(numberPosts)
        days = self.day_list([start_date, end_date])
        # basecase
        if numberPosts < 10000:
            url = "https://api.crimsonhexagon.com/api/monitor/posts"
            querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                           "start": start_date, "end": end_date}
            if "filter" in kwargs:
                querystring["filter"] = kwargs["filter"]
            if "geotagged" in kwargs:
                querystring["geotagged"] = kwargs["geotagged"]
            querystring["extendLimit"] = 'True'
            querystring["fullContents"] = 'True'
            response = requests.request("GET", url, params=querystring)
            if response.text:
                json_data = json.loads(response.text)
                if 'posts' in json_data.keys():
                    posts = json_data['posts']
                    return posts
        elif len(days) == 2:
            url = "https://api.crimsonhexagon.com/api/monitor/posts"
            querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                           "start": start_date, "end": end_date}
            if "filter" in kwargs:
                querystring["filter"] = kwargs["filter"]
            if "geotagged" in kwargs:
                querystring["geotagged"] = kwargs["geotagged"]
            querystring["extendLimit"] = 'True'
            querystring["fullContents"] = 'True'
            response = requests.request("GET", url, params=querystring)
            if response.text:
                json_data = json.loads(response.text)
                if 'posts' in json_data.keys():
                    posts = json_data['posts']
                    logging.warning('10,000 posts per day limit has been enforced for {0}'.format(start_date))
                    return posts
        elif len(days) == 3:
            posts1 = self.recursive_pull(start_date, days[1], df_numPosts)
            posts2 = self.recursive_pull(days[1], end_date, df_numPosts)
            return posts1 + posts2
        else:
            half = math.ceil(len(days) / 2)
            posts1 = self.recursive_pull(start_date, days[half], df_numPosts)
            posts2 = self.recursive_pull(days[half], end_date, df_numPosts)
            return posts1 + posts2
        return []

    def get_monitor_posts_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/results"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)['results']
        volumeData = self.json_2_pandas_results(json_data)
        volumeData["startDate"] = pd.to_datetime(volumeData["startDate"])
        volumeData["endDate"] = pd.to_datetime(volumeData["endDate"])
        #         volumeData["startDate"] = volumeData["startDate"].dt.date
        #         volumeData["endDate"] = volumeData["endDate"].dt.date
        url = "https://api.crimsonhexagon.com/api/monitor/posts"
        totalposts = []
        totalposts.append(self.recursive_pull(start_date, end_date, volumeData))
        totalposts = list(itertools.chain.from_iterable(totalposts))
        return totalposts

    def create_empty_post_df(self):
        df = self.create_empty_df(
            ['url', 'date', 'author', 'title', 'contents', 'type', 'location',
             'geolocation_id', 'geolocation_name', 'geolocation_country', 'geolocation_state',
             'language', 'authorGender', 'Basic_Positive', 'Basic_Negative', 'Basic_Neutral',
             'Emotion_Joy', 'Emotion_Sadness', 'Emotion_Anger', 'Emotion_Disgust',
             'Emotion_Surprise', 'Emotion_Fear', 'Emotion_Neutral'],
            dtypes=[np.str, np.str, np.str, np.str, np.str, np.str, np.str,
                    np.str, np.str, np.str, np.str, np.str,
                    np.str, np.float, np.float, np.float,
                    np.float, np.float, np.float, np.float,
                    np.float, np.float, np.float]
        )
        return df

    def post_row_2_df_row(self, postRow, data):
        row = {}
        if 'url' in postRow.keys():
            row['url'] = postRow['url']
        else:
            row['url'] = ''
        if 'date' in postRow.keys():
            row['date'] = postRow['date']
        else:
            row['date'] = ''
        try:
            row['author'] = postRow['author'].encode('utf-8')
        except:
            row['author'] = ''
        try:
            row['title'] = postRow['title'].encode('utf-8')
        except:
            row['title'] = ''
        if 'type' in postRow.keys():
            row['type'] = postRow['type']
        else:
            row['type'] = ''
        if 'location' in postRow.keys():
            row['location'] = postRow['location']
        else:
            row['location'] = ''
        try:
            if postRow['type'] == 'Reddit':
                print(postRow['contents'])
            row['contents'] = postRow['contents'].encode('utf-8')
        except:
            row['contents'] = ''
        if 'geolocation' in postRow.keys():
            if 'id' in postRow['geolocation'].keys():
                row['geolocation_id'] = postRow['geolocation']['id']
        else:
            row['geolocation_id'] = ''
        if 'geolocation' in postRow.keys():
            if 'name' in postRow['geolocation'].keys():
                row['geolocation_name'] = postRow['geolocation']['name']
        else:
            row['geolocation_name'] = ''

        if 'geolocation' in postRow.keys():
            if 'country' in postRow['geolocation'].keys():
                row['geolocation_name'] = postRow['geolocation']['country']
        else:
            row['geolocation_country'] = ''

        if 'geolocation' in postRow.keys():
            if 'state' in postRow['geolocation'].keys():
                row['geolocation_state'] = postRow['geolocation']['state']
        else:
            row['geolocation_state'] = ''
        if 'language' in postRow.keys():
            row['language'] = postRow['language']
        else:
            row['language'] = ''
        if 'authorGender' in postRow.keys():
            row['authorGender'] = postRow['authorGender']
        else:
            row['authorGender'] = ''
        try:
            row['Basic_Positive'] = postRow['categoryScores'][0]['score']
            row['Basic_Negative'] = postRow['categoryScores'][1]['score']
            row['Basic_Neutral'] = postRow['categoryScores'][2]['score']

        except:
            row['Basic_Positive'] = ''
            row['Basic_Negative'] = ''
            row['Basic_Neutral'] = ''

        try:
            row['Emotion_Anger'] = postRow['emotionScores'][0]['score']
            row['Emotion_Joy'] = postRow['emotionScores'][1]['score']
            row['Emotion_Sadness'] = postRow['emotionScores'][2]['score']
            row['Emotion_Disgust'] = postRow['emotionScores'][3]['score']
            row['Emotion_Surprise'] = postRow['emotionScores'][4]['score']
            row['Emotion_Fear'] = postRow['emotionScores'][5]['score']
            row['Emotion_Neutral'] = postRow['emotionScores'][6]['score']

        except:
            row['Emotion_Joy'] = ''
            row['Emotion_Sadness'] = ''
            row['Emotion_Anger'] = ''
            row['Emotion_Disgust'] = ''
            row['Emotion_Surprise'] = ''
            row['Emotion_Fear'] = ''
            row['Emotion_Neutral'] = ''

        data.insert(0, row)

    def json_2_pandas_posts(self, json_data):
        # returnDf = self.create_empty_post_df()
        data = []
        for post in json_data:
            self.post_row_2_df_row(post, data)
        returnDf = pd.DataFrame(data)
        return returnDf

    # ********************************************************** ###
    # ********************************************************** ###
    # ***                  Volume                            *** ###
    # ***                                                    *** ###
    # ***  Returns volume metrics for a given monitor        *** ###
    # *** split by hour, day, week or month. Week and month  *** ###
    # *** aggregations requires a date range of at least 1   *** ###
    # *** full unit; e.g., WEEKLY requires a date range of   *** ###
    # *** at least 1 week;. Additionally, these              *** ###
    # *** aggregations only returns full units so the range  *** ###
    # *** may be truncated. e.g., 2017-01-15 to 2017-03-15   *** ###
    # *** with MONTHLY grouping will return a date range     *** ###
    # *** of 2017-02-01 to 2017-03-01. A monitor must have   *** ###
    # *** complete results for the specified date range.     *** ###
    # *** If any day in the range is missing results an      *** ###
    # *** error will be returned.                            *** ###
    # ***                                                    *** ###
    # ********************************************************** ###
    # ********************************************************** ###

    def get_monitor_volume_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/volume"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        if "groupBy" in kwargs:
            querystring["groupBy"] = kwargs["groupBy"]
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)
        return json_data

    def get_sub_char_groupby_volume(self, data):
        if data['groupBy'] == 'HOURLY':
            return 'h'
        elif data['groupBy'] == 'DAILY':
            return 'd'
        elif data['groupBy'] == 'WEEKLY':
            return 'w'
        elif data['groupBy'] == 'MONTHLY':
            return 'm'
        else:
            return ''

    def volumes_2_dict_list(self, json_data, data):
        tempData = {}
        for timeframe in json_data['volume']:
            tempData['totalStartDate'] = json_data['startDate']
            tempData['totalEndDate'] = json_data['endDate']
            tempData['timezone'] = json_data['timezone']
            tempData['groupBy'] = json_data['groupBy']
            tempData['totalNumberOfDocuments'] = json_data['numberOfDocuments']

            tempData['startDate'] = timeframe['startDate']
            tempData['endDate'] = timeframe['endDate']
            tempData['numberOfDocuments'] = timeframe['numberOfDocuments']
            data.append(tempData)
            tempData = {}



    def json_2_pandas_volume(self, json_data):
        data = []
        self.volumes_2_dict_list(json_data, data)
        returnDf = pd.DataFrame(data)
        return returnDf

    # ********************************************************** ###
    # ********************************************************** ###
    # ***                  Volume by Day and Time            *** ###
    # ***  Buzz, Opinion & Social Account Monitors           *** ###
    # ***                                                    *** ###
    # *** Returns volume information for a given monitor     *** ###
    # *** aggregated by time of day or day of week. A monitor*** ###
    # *** must have complete results for the specified date  *** ###
    # *** range. If any day in the range is missing results  *** ###
    # *** an error will be returned.                         *** ###
    # ***                                                    *** ###
    # ********************************************************** ###
    # ********************************************************** ###

    def get_monitor_volume_by_dt_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/dayandtime"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        if "aggregatedbyday" in kwargs:
            querystring["aggregatedbyday"] = kwargs["aggregatedbyday"]
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)
        return json_data

    def volumes_by_dt_2_dict_list(self, json_data, data):
        tempData = {}
        for timeframe in json_data['volumes']:
            tempData['startDate'] = timeframe['startDate']
            tempData['endDate'] = timeframe['endDate']
            tempData['numberOfDocuments'] = timeframe['numberOfDocuments']
            for key, value in timeframe['volume'].items():
                tempData['volume_' + str(key)] = value
            data.append(tempData)
            tempData = {}



    def json_2_pandas_volume_by_dt(self, json_data):
        data = []
        self.volumes_by_dt_2_dict_list(json_data, data)
        returnDf = pd.DataFrame(data)
        return returnDf

    # ********************************************************** ###
    # ********************************************************** ###
    # ***                  Word Cloud                        *** ###
    # ***  Buzz, Opinion & Social Account Monitors           *** ###
    # ***                                                    *** ###
    # *** The Word Cloud endpoint returns an alphabetized    *** ###
    # *** list of the top 300 words in a monitor. This data  *** ###
    # *** is generated using documents randomly selected     *** ###
    # *** from the pool defined by the submitted parameters. *** ###
    # ***                                                    *** ###
    # ********************************************************** ###
    # ********************************************************** ###

    def get_monitor_wordcloud_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/wordcloud"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)
        return json_data

    def wordcloud_row_2_df_row(self, postRow, data):
        tempData = {}
        for word, value in postRow['data'].items():
            tempData['word'] = word
            tempData['weight'] = value
            data.append(tempData)
            tempData = {}
        row = {}

    def json_2_pandas_wordcloud(self, json_data):
        data = []
        self.wordcloud_row_2_df_row(json_data, data)
        returnDf = pd.DataFrame(data)
        return returnDf

    # ********************************************************** ###
    # ********************************************************** ###
    # ***                  Age                               *** ###
    # ***  Buzz, Opinion & Social Account Monitors           *** ###
    # ***                                                    *** ###
    # *** Returns volume metrics for a given monitor split   *** ###
    # *** monitor split by age bracket.                      *** ###
    # ***                                                    *** ###
    # ********************************************************** ###
    # ********************************************************** ###

    def get_monitor_age_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/demographics/age"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)
        return json_data

    def create_empty_age_df(self):
        df = self.create_empty_df(
            ['startDate', 'endDate', 'numberOfDocuments', 'ZERO_TO_SEVENTEEN', 'EIGHTEEN_TO_TWENTYFOUR',
             'TWENTYFIVE_TO_THIRTYFOUR', 'THIRTYFIVE_AND_OVER'
                , 'totalAgeCount'],
            dtypes=[np.str, np.str, np.float, np.float, np.float, np.float, np.float, np.float]
        )
        return df

    def age_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'startDate' in resultsRow.keys():
            row['startDate'] = resultsRow['startDate']
        else:
            row['startDate'] = ''
        if 'endDate' in resultsRow.keys():
            row['endDate'] = resultsRow['endDate']
        else:
            row['endDate'] = ''
        if 'numberOfDocuments' in resultsRow.keys():
            row['numberOfDocuments'] = resultsRow['numberOfDocuments']
        else:
            row['numberOfDocuments'] = ''
        if 'ageCount' in resultsRow.keys() and 'sortedAgeCounts' in resultsRow['ageCount']:
            if 'ZERO_TO_SEVENTEEN' in resultsRow['ageCount']['sortedAgeCounts']:
                row['ZERO_TO_SEVENTEEN'] = resultsRow['ageCount']['sortedAgeCounts']['ZERO_TO_SEVENTEEN']
            else:
                row['ZERO_TO_SEVENTEEN'] = ''
            if 'EIGHTEEN_TO_TWENTYFOUR' in resultsRow['ageCount']['sortedAgeCounts']:
                row['EIGHTEEN_TO_TWENTYFOUR'] = resultsRow['ageCount']['sortedAgeCounts']['EIGHTEEN_TO_TWENTYFOUR']
            else:
                row['EIGHTEEN_TO_TWENTYFOUR'] = ''
            if 'TWENTYFIVE_TO_THIRTYFOUR' in resultsRow['ageCount']['sortedAgeCounts']:
                row['TWENTYFIVE_TO_THIRTYFOUR'] = resultsRow['ageCount']['sortedAgeCounts']['TWENTYFIVE_TO_THIRTYFOUR']
            else:
                row['TWENTYFIVE_TO_THIRTYFOUR'] = ''
            if 'THIRTYFIVE_AND_OVER' in resultsRow['ageCount']['sortedAgeCounts']:
                row['THIRTYFIVE_AND_OVER'] = resultsRow['ageCount']['sortedAgeCounts']['THIRTYFIVE_AND_OVER']
            else:
                row['THIRTYFIVE_AND_OVER'] = ''
        if 'totalAgeCount' in resultsRow.keys():
            row['totalAgeCount'] = resultsRow['ageCount']['totalAgeCount']
        else:
            row['totalAgeCount'] = ''
        data.insert(0, row)

    def json_2_pandas_age(self, json_data):
        data = []
        for day in json_data['ageCounts']:
            self.age_row_2_df_row(day, data)
        retdf = pd.DataFrame.from_records(data)
        return retdf

    # ********************************************************** ###
    # ********************************************************** ###
    # ***                  Gender                            *** ###
    # ***  Buzz, Opinion & Social Account Monitors           *** ###
    # ***                                                    *** ###
    # *** Returns volume metrics for a given monitor split   *** ###
    # *** monitor split by gender.                           *** ###
    # ***                                                    *** ###
    # ********************************************************** ###
    # ********************************************************** ###

    def get_monitor_gender_request(self, start_date, end_date, **kwargs):
        url = "https://api.crimsonhexagon.com/api/monitor/demographics/gender"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                       "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        json_data = json.loads(response.text)
        return json_data

    def create_empty_gender_df(self):
        df = self.create_empty_df(
            ['startDate', 'endDate', 'numberOfDocuments', "maleCount",
             "femaleCount",
             "totalGenderedCount",
             "percentMale",
             "percentFemale"],
            dtypes=[np.str, np.str, np.float, np.float, np.float, np.float, np.float, np.float]
        )
        return df

    def gender_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'startDate' in resultsRow.keys():
            row['startDate'] = resultsRow['startDate']
        else:
            row['startDate'] = ''
        if 'endDate' in resultsRow.keys():
            row['endDate'] = resultsRow['endDate']
        else:
            row['endDate'] = ''
        if 'numberOfDocuments' in resultsRow.keys():
            row['numberOfDocuments'] = resultsRow['numberOfDocuments']
        else:
            row['numberOfDocuments'] = ''
        if 'maleCount' in resultsRow.keys():
            row['maleCount'] = resultsRow['genderCounts']['maleCount']
        else:
            row['maleCount'] = ''
        if 'femaleCount' in resultsRow.keys():
            row['femaleCount'] = resultsRow['genderCounts']['femaleCount']
        else:
            row['femaleCount'] = ''
        if 'totalGenderedCount' in resultsRow.keys():
            row['totalGenderedCount'] = resultsRow['genderCounts']['totalGenderedCount']
        else:
            row['totalGenderedCount'] = ''
        if 'percentMale' in resultsRow.keys():
            row['percentMale'] = resultsRow['genderCounts']['percentMale']
        else:
            row['percentMale'] = ''
        if 'percentFemale' in resultsRow.keys():
            row['percentFemale'] = resultsRow['genderCounts']['percentFemale']
        else:
            row['percentFemale'] = ''
        data.insert(0, row)

    def json_2_pandas_gender(self, json_data):
        returnDf = self.create_empty_gender_df()
        data = []
        for day in json_data['genderCounts']:
            self.gender_row_2_df_row(day, data)
            returnDf = pd.concat([pd.DataFrame(data), returnDf], ignore_index=True, sort=False)
        if 'returnDf' in locals():
            return returnDf

########################################
    
    def get_twitter_metrics(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/twittermetrics"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def twitter_metrics_row_2_df_row(self, resultsRow, data):
        row = {}
        date = resultsRow['startDate']
        if 'topHashtags' in resultsRow.keys():
            for hashtag in resultsRow['topHashtags']:
                row['date'] = date
                row['content'] = hashtag
                row['type'] = 'hashtag'
                row['count'] = resultsRow['topHashtags'][hashtag]
                data.append(row)
        row = {}
        if 'topMentions' in resultsRow.keys():
            for mention in resultsRow['topMentions']:
                row['date'] = date
                row['content'] = mention
                row['type'] = 'mention'
                row['count'] = resultsRow['topMentions'][mention]
                data.append(row)
        row = {}
        if 'topRetweets' in resultsRow.keys():
            for retweet in resultsRow['topRetweets']:
                row['date'] = date
                row['content'] = retweet['url']
                row['type'] = 'retweet'
                row['count'] = retweet['retweetCount']
                data.append(row)

    def json_2_pandas_twitter_metrics(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.twitter_metrics_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

########################################

    def get_twitter_sentposts(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/twittersocial/sentposts"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def twitter_sentposts_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'sentPostMetrics' in resultsRow.keys() and len(resultsRow['sentPostMetrics']) > 0:
            for metric in resultsRow['sentPostMetrics']:
                row['url'] = metric['url']
                row['date'] = metric['date']
                row['retweets'] = metric['retweets']
                row['replies'] = metric['replies']
                row['impressions'] = metric['impressions']
                row['content'] = metric['content']
                data.append(row)
                row = {}

    def json_2_pandas_sentposts(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.twitter_sentposts_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################

    def get_twitter_followers(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/twittersocial/followers"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def twitter_followers_row_2_df_row(self, resultsRow, data):
        row = {}
        row['date'] = resultsRow['date']
        row['followers'] = resultsRow['followers']
        data.append(row)
        row = {}

    def json_2_pandas_twitter_followers(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.twitter_followers_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################

    def get_facebook_admin_posts(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/facebook/adminposts"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def facebook_admin_posts_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'adminPostMetrics' in resultsRow.keys() and len(resultsRow['adminPostMetrics']) > 0:
            for metric in resultsRow['adminPostMetrics']:
                row['content'] = metric['content']
                row['url'] = metric['url']
                row['date'] = metric['date']
                row['postLikes'] = metric['postLikes']
                row['postShares'] = metric['postShares']
                row['postComments'] = metric['postComments']
                row['isLocked'] = metric['isLocked']
                data.append(row)
                row = {}

    def json_2_pandas_facebook_admin_posts(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.facebook_admin_posts_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################
############################################################

    def get_facebook_page_likes(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/facebook/pagelikes"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def facebook_page_likes_row_2_df_row(self, resultsRow, data):
        row = {}
        row['date'] = resultsRow['date']
        row['likes'] = resultsRow['likes']
        data.append(row)

    def json_2_pandas_facebook_page_likes(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.facebook_page_likes_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################
############################################################

    def get_facebook_total_activity(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/facebook/totalactivity"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def facebook_total_activity_row_2_df_row(self, resultsRow, data):
        row = {}
        row['date'] = resultsRow['startDate']
        admin = resultsRow['admin']
        user = resultsRow['user']
        if len(admin)>0:
            row['adminPosts'] = admin['adminPosts']
            row['likesOnAdmin'] = admin['likesOnAdmin']
            row['commentsOnAdmin'] = admin['commentsOnAdmin']
            row['sharesOnAdmin'] = admin['sharesOnAdmin']
        if len(user)>0:
            row['userPosts'] = user['userPosts']
            row['likesOnUser'] = user['likesOnUser']
            row['commentsOnUser'] = user['commentsOnUser']
            row['sharesOnUser'] = user['sharesOnUser']
        data.append(row)

    def json_2_pandas_facebook_total_activity(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.facebook_total_activity_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################
############################################################

    def get_instagram_followers(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/instagram/followers"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def instagram_followers_row_2_df_row(self, resultsRow, data):
        row = {}
        row['date'] = resultsRow['date']
        row['followerCount'] = resultsRow['followerCount']
        data.append(row)

    def json_2_pandas_instagram_followers(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.instagram_followers_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf

############################################################
############################################################

    def get_instagram_sent_media(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/instagram/sentmedia"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def instagram_sent_media_row_2_df_row(self, resultsRow, data):
        row = {}
        if 'adminPostMetrics' in resultsRow.keys() and len(resultsRow['adminPostMetrics']) > 0:
            for metric in resultsRow['adminPostMetrics']:
                row['content'] = metric['content']
                row['url'] = metric['url']
                row['date'] = metric['date']
                row['postLikes'] = metric['postLikes']
                row['postComments'] = metric['postComments']
                row['isLocked'] = metric['isLocked']
                data.append(row)
                row = {}

    def json_2_pandas_instagram_sent_media(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.instagram_sent_media_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf


############################################################
############################################################

    def get_instagram_total_activity(self, start_date, end_date):
        url = "https://api.crimsonhexagon.com/api/monitor/instagram/totalactivity"
        querystring = {"auth": self.authenticationToken, "id": self.monitorId,
                    "start": start_date, "end": end_date}
        response = requests.request("GET", url, params=querystring)
        try:
            json_data = json.loads(response.text)
            return json_data
        except Exception as E:
            print(response.text)
            print(E)
            return

    def instagram_total_activity_row_2_df_row(self, resultsRow, data):
        row = {}
        row['date'] = resultsRow['startDate']
        if 'admin' in resultsRow.keys() and len(resultsRow['admin']) > 0:
            admin = resultsRow['admin']
            row['adminPosts'] = admin['adminPosts']
            row['likesOnAdmin'] = admin['likesOnAdmin']
            row['commentsOnAdmin'] = admin['commentsOnAdmin']
            data.append(row)
            row = {}

    def json_2_pandas_instagram_total_activity(self, json_data):
        data = []
        for day in json_data['dailyResults']:
            self.instagram_total_activity_row_2_df_row(day, data)
            returnDf = pd.DataFrame(data)
        if 'returnDf' in locals():
            return returnDf
