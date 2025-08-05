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

    def get_exp_params(self, exp_info: dict, date: str, exp_end_dt: str) -> dict:
        return dict({
                "exp_id": exp_info["id"],
                "date": date,
                'datetime_start': exp_info["date_start"],
                'datetime_end': exp_end_dt,
                "exposure_event": exp_info["experiment_event_start"],
                # "exposure_event": "Explore Open",
                # "platform": "Mobile",
                "platform": "all",
                "include_values": self.generate_sql_list_filter("value", []),
                "exclude_values": self.generate_sql_list_filter("value", [], exclude=True),
                'pro_rights': self.generate_sql_rights_filter("pro", "Free"),
                # 'pro_rights': self.generate_sql_rights_filter("pro", "Empty"),
                # 'pro_rights': self.generate_sql_rights_filter("pro", "All"),
                'edu_rights': self.generate_sql_rights_filter("edu", "All"),
                'sing_rights': self.generate_sql_rights_filter("sing", "All"),
                'practice_rights': self.generate_sql_rights_filter("practice", "All"),
                'book_rights': self.generate_sql_rights_filter("book", "All"),
                "country": "all",
                # "country": "US",
                "source": exp_info["calc_source"],
                "custom_where": 1,
                "custom_sub_where": 1,
                'custom_having': 1,
                "custom_sub_having": 1,
                # "funnel_source_include": "Tour Install",
                "funnel_source_include": self.generate_sql_list_filter("funnel_source", ["Tour Install"], exclude=False),
                # "funnel_source_include": "Export2pdfDownload",
                # "funnel_source_include": "AD Interstitial",
                # "funnel_source_include": "Tour Instant Offer",
                "funnel_source_exclude": self.generate_sql_list_filter("funnel_source", [], exclude=True),
                # "funnel_source_exclude": "'Tour Install', 'Tour Instant Offer'",
                # "funnel_source_exclude": "'AD Interstitial'",
                "variations": [variation + 1 for variation in range(exp_info["variations"])],
                "platform_suffix": "web" if exp_info["calc_source"] == "UG_WEB" else "app"
            })


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


    def generate_sql_list_filter(self, column: str, values: list, exclude: bool = False) -> str:
        if values == []:
            return "1=1"
        values_list = ", ".join([f"'{v}'" for v in values])
        if exclude:
            return f"{column} not in ({values_list})"
        return f"{column} in ({values_list})"


    def get_query(self, query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req


    def get_experiment(self, id) -> dict:
        query = self.get_query("get_exp_info", params=dict({"id": id}))
        print(query)
        query_result = self._mb_client.post("dataset", query)
        df = query_result
        print(df)
        clients_pattern = r'(\w+)'
        df['clients_list'] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
        exp_info: dict = {
            "id": df.id[0],
            "date_start": df.date_start[0],
            "date_end": df.date_end[0],
            "variations": df.variations[0],
            "experiment_event_start": df.experiment_event_start[0],
            "configuration": df.configuration[0],
            'clients_list': df.clients_list[0],
            'clients_options': df.clients_options[0]
        }
        return exp_info


    def build_monetization_query(self, platform_suffix: str, ex_subs_filter: str) -> str:
        if ex_subs_filter == "":
            query = f"""
                with vars as (
                    {self.get_query("date_variation")}
                ),
                members as (
                    {self.get_query(f"members_{platform_suffix}")}
                ),
                subscriptions as (
                    {self.get_query("subscriptions")}
                ),
                bydate as (
                    {self.get_query("monetization_metrics")}
                )
                select * from vars left join bydate using(dt, variation)
            """
        else:
            query = f"""
                with vars as (
                    {self.get_query("date_variation")}
                ),
                members as (
                    select distinct
                        m.variation as variation,
                        m.unified_id as unified_id,
                        m.user_id as user_id,
                        m.session_id as session_id,
                        m.exp_start_dt as exp_start_dt,
                        m.rights as rights,
                        m.country as country,
                        m.source as source
                    from (
                        {self.get_query(f"members_{platform_suffix}")}
                    ) as m
                    inner join (
                        {self.get_query(f"subs_filter")} ) as s
                    using(user_id)
                    where ({ex_subs_filter})
                ),
                subscriptions as (
                    {self.get_query("subscriptions")}
                ),
                bydate as (
                    {self.get_query("monetization_metrics")}
                )
                select * from vars left join bydate using(dt, variation)
            """
        return query


    def build_retention_query(self, platform_suffix: str):
        query = f"""
            with vars as (
                {self.get_query("date_variation")}
            ),
            members as (
                {self.get_query(f"members_{platform_suffix}")}
            ),
            bydate as (
                {self.get_query(f"retention_{platform_suffix}")}
            )
            select * from vars left join bydate using(dt, variation)
        """
        return query

    def build_long_tab_view_query(self, platform_suffix: str):
        query = f"""
            with vars as (
                {self.get_query("date_variation")}
            ),
            members as (
                {self.get_query(f"members_{platform_suffix}")}
            ),
            {self.get_query(f"long_tab_view_{platform_suffix}")}
        """
        return query


    def build_dau_query(self):
        query = self.get_query("dau")
        return query


    def get_exp_daily_monetization_data(self, params: dict = {}):
        monetization_query = self.build_monetization_query(params["platform_suffix"], params["ex_subs_filter"]).format(**params)
        # print(monetization_query)
        query_result = self._mb_client.post("dataset", monetization_query)
        return query_result


    def get_exp_daily_retention_data(self, params: dict = {}):
        retention_query = self.build_retention_query(params["platform_suffix"]).format(**params)
        # print(retention_query) 
        query_result = self._mb_client.post("dataset", retention_query)
        return query_result


    def get_exp_daily_long_tab_view_data(self, params: dict = {}):
        long_tab_view_query = self.build_long_tab_view_query(params["platform_suffix"]).format(**params)
        # print(long_tab_view_query) 
        query_result = self._mb_client.post("dataset", long_tab_view_query)
        return query_result


    def get_exp_monetization_data(self, exp_info):
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
        exp_end_dt = datetime.datetime.now(datetime.timezone.utc)
        exp_end_dt_param = int(datetime.datetime.timestamp(exp_end_dt))
        if exp_info["date_end"] > exp_info["date_start"]:
            exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_end"], datetime.timezone.utc)
            exp_end_dt_param = exp_info["date_end"]
        days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days + 1
        for day in range(days_cnt):
            current_day = exp_start_dt + datetime.timedelta(days=day)
            params = self.get_exp_params(exp_info, current_day.strftime("%Y-%m-%d"), exp_end_dt_param)
            # params["ex_subs_filter"] = ""
            # params["ex_subs_filter"] = """
            # s.sub_dt > toDateTime(0)
            # and s.can_dt > s.sub_dt
            # and s.can_dt <= toDateTime(m.exp_start_dt)
            # and
            #     product_id in (
            #     'com.ultimateguitar.ugt.pro_edu_sing.1year12',
            #     'com.ultimateguitar.ugt.pro_edu_sing.instant.1year8',
            #     'com.ultimateguitar.ugt.pro_edu_sing.1year11',
            #     'com.ultimateguitar.ugt.pro_edu_sing.instant.1year7',
            #     'com.ultimateguitar.ugt.plus.1year2',
            #     'com.ultimateguitar.tabs.plus.1year2',
            #     'com.ultimateguitar.ugt.plus.instant.1year2',
            #     'com.ultimateguitar.tabs.plus.inst.1year2'
            # )
            # """
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
            params = self.get_exp_params(exp_info, current_day.strftime("%Y-%m-%d"), exp_end_dt_param)
            params["members"] = "members"
            df = self.get_exp_daily_retention_data(params)
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df


    def get_exp_long_tab_view_data(self, exp_info):
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
            params = self.get_exp_params(exp_info, current_day.strftime("%Y-%m-%d"), exp_end_dt_param)
            params["members"] = "members"
            df = self.get_exp_daily_long_tab_view_data(params)
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df


    def get_dau_data(self, exp_info):
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
            params = self.get_exp_params(exp_info, current_day.strftime("%Y-%m-%d"), exp_end_dt_param)
            # print(self.build_dau_query().format(**params))
            df = self._mb_client.post("dataset", self.build_dau_query().format(**params))
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df
