from dotenv import dotenv_values
import pandas as pd
import datetime
import math
import re

from metabase import Mb_Client



class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}",
            username=secrets["username"],
            password=secrets["password"]
        )


    def generate_sql_rights_filter(self, rights_type: str, rights: str):
        rights_level_list = ['pro', 'edu', 'sing', 'practice', 'book']
        rights_level = int(math.pow(10, rights_level_list.index(rights_type)))
        rights_dict: dict = {
            'Empty': f'toUInt32(rights / {rights_level}) % 10 = 0',
            'Free': f'toUInt32(rights / {rights_level}) % 10 in (0, 4, 5)',
            'Finite subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2)',
            'Lifetime': f'toUInt32(rights / {rights_level}) % 10 in (3)',
            'Any paid': f'toUInt32(rights / {rights_level}) % 10 in (2, 3)',
            'Any subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2, 3)',
            'Trial': f'toUInt32(rights / {rights_level}) % 10 in (1)',
            'Expired subscription': f'toUInt32(rights / {rights_level}) % 10 in (5)',
            'Expired trial': f'toUInt32(rights / {rights_level}) % 10 in (4)',
            'Expired any': f'toUInt32(rights / {rights_level}) % 10 in (4, 5)',
            'All': f'1'
        }
        return rights_dict[rights]


    def get_query(self, query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req


    def get_experiment(self, id) -> dict:
        query = self.get_query("get_exp_info", params=dict({"id": id}))
        query_result = self._mb_client.post("dataset", query)
        df = query_result
        clients_pattern = r'(\w+)'
        df['clients_list'] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
        exp_info: dict = {
            "id": df.id[0],
            "date_start": df.date_start[0],
            "date_end": df.date_end[0],
            "variations": df.variations[0],
            "experiment_event_start": df.experiment_event_start[0],
            "configuration": df.configuration[0],
            'clients_list': df.clients_list[0]
        }
        return exp_info


    def build_monetization_query(self):
        query = f"""
            with members as (
                {self.get_query("members")}
            ),
            subscriptions as (
                {self.get_query("subscriptions")}
            ),
            bydate as (
                 {self.get_query("monetization_metrics")}
            )
            select * from bydate
        """
        return query


    def build_retention_query(self):
        query = f"""
            with members as (
                {self.get_query("members")}
            ),
            bydate as (
                 {self.get_query("retention_app")}
            )
            select * from bydate
        """
        return query


    def get_exp_daily_monetization_data(self, params: dict = {}):
        monetization_query = self.build_monetization_query().format(**params)
        # print(monetization_query)
        query_result = self._mb_client.post("dataset", monetization_query)
        return query_result


    def get_exp_daily_retention_data(self, params: dict = {}):
        retention_query = self.build_retention_query().format(**params)
        # print(retention_query) 
        query_result = self._mb_client.post("dataset", retention_query)
        return query_result


    def get_exp_monetization_data(self, exp_info):
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
        exp_end_dt = datetime.datetime.now(datetime.timezone.utc)
        exp_end_dt_param =  int(datetime.datetime.timestamp(exp_end_dt))
        if exp_info["date_end"] > exp_info["date_start"]:
            exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_end"], datetime.timezone.utc)
            exp_end_dt_param = exp_info["date_end"]
        days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days + 1
        for day in range(days_cnt):
            current_day = exp_start_dt + datetime.timedelta(days=day)
            params = dict({
                "exp_id": exp_info["id"],
                "date": current_day.strftime("%Y-%m-%d"),
                'datetime_start': exp_info["date_start"],
                'datetime_end': exp_end_dt_param,
                # "exposure_event": exp_info["experiment_event_start"],
                # "exposure_event": "Banner Upgrade View",
                "exposure_event": "App Experiment Start",
                # "platform": "Mobile",
                "platform": "all",
                "include_values": "",
                "exclude_values": "",
                # 'pro_rights': self.generate_sql_rights_filter("pro", "Free"),
                'pro_rights': self.generate_sql_rights_filter("pro", "Empty"),
                'edu_rights': self.generate_sql_rights_filter("edu", "All"),
                'sing_rights': self.generate_sql_rights_filter("sing", "All"),
                'practice_rights': self.generate_sql_rights_filter("practice", "All"),
                'book_rights': self.generate_sql_rights_filter("book", "All"),
                "country": "all",
                # "country": "US",
                "source": "UGT_IOS",
                # "source": "UGT_ANDROID",
                "custom_where": 1,
                "custom_sub_where": 1,
                'custom_having': 1,
                "custom_sub_having": 1,
                # "funnel_source_include": "AD Interstitial",
                "funnel_source_include": "",
                "funnel_source_exclude": ""
                # "funnel_source_exclude": "AD Interstitial"
            })
            df = self.get_exp_daily_monetization_data(params)
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df

    def get_exp_retention_data(self, exp_info):
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
        exp_end_dt = datetime.datetime.now(datetime.timezone.utc)
        exp_end_dt_param =  int(datetime.datetime.timestamp(exp_end_dt))
        if exp_info["date_end"] > exp_info["date_start"]:
            exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_end"], datetime.timezone.utc)
            exp_end_dt_param = exp_info["date_end"]
        days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days + 1
        for day in range(days_cnt):
            current_day = exp_start_dt + datetime.timedelta(days=day)
            params = dict({
                "exp_id": exp_info["id"],
                "date": current_day.strftime("%Y-%m-%d"),
                'datetime_start': exp_info["date_start"],
                'datetime_end': exp_end_dt_param,
                "exposure_event": "App Experiment Start",
                "platform": "all",
                "include_values": "",
                "exclude_values": "",
                'pro_rights': self.generate_sql_rights_filter("pro", "Empty"),
                'edu_rights': self.generate_sql_rights_filter("edu", "All"),
                'sing_rights': self.generate_sql_rights_filter("sing", "All"),
                'practice_rights': self.generate_sql_rights_filter("practice", "All"),
                'book_rights': self.generate_sql_rights_filter("book", "All"),
                "country": "all",
                "source": "UGT_IOS",
                "custom_where": 1,
                'custom_having': 1,
                "retention_events": "'Tab Open', 'App Start', 'Courses Open', 'Shots Open', 'Tabs Open'",
                "members": "members"
            })
            df = self.get_exp_daily_retention_data(params)
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df