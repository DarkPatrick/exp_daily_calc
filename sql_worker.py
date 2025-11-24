from dotenv import dotenv_values
import pandas as pd
import datetime
import math
import re
from requests.structures import CaseInsensitiveDict

from metabase import Mb_Client
from sql_custom_funnel_generator import generate_clickhouse_sql



class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}", 
            username=secrets["username"],
            password=secrets["password"],
            api_key=secrets.get("mb_api_key", "")
        )
        self._current_segment: CaseInsensitiveDict = CaseInsensitiveDict()
        self._funnels: dict = {}


    def get_exps_to_calc(self) -> pd.DataFrame:
        pass

    def get_exp_params(self, exp_info: dict, date: str, exp_end_dt: str) -> dict:
        return dict({
                "exp_id": exp_info["id"],
                "date": date,
                'datetime_start': self._current_segment.get("datetime_start", exp_info["date_start"]),
                'datetime_end': self._current_segment.get("datetime_end", exp_end_dt),
                "exposure_event": self._current_segment.get("exposure_event", exp_info["experiment_event_start"]),
                "platform": self._current_segment.get("platform", "all").lower(),
                "include_values": self.generate_sql_list_filter("value", self._current_segment.get("include_values", [])),
                "exclude_values": self.generate_sql_list_filter("value", self._current_segment.get("exclude_values", []), exclude=True),
                'pro_rights': self.generate_sql_rights_filter("pro", self._current_segment.get("pro_rights", "all").lower()),
                'edu_rights': self.generate_sql_rights_filter("edu", self._current_segment.get("edu_rights", "all").lower()),
                'sing_rights': self.generate_sql_rights_filter("sing", self._current_segment.get("sing_rights", "all").lower()),
                'practice_rights': self.generate_sql_rights_filter("practice", self._current_segment.get("practice_rights", "all").lower()),
                'book_rights': self.generate_sql_rights_filter("book", self._current_segment.get("book_rights", "all").lower()),
                "country": self._current_segment.get("country", "all"),
                "source": exp_info["calc_source"],
                "custom_where": self._current_segment.get("custom_where", 1),
                "custom_sub_where": self._current_segment.get("custom_sub_where", 1),
                'custom_having': self._current_segment.get("custom_having", 1),
                "custom_sub_having": self._current_segment.get("custom_sub_having", 1),
                "funnel_source_include": self.generate_sql_list_filter("funnel_source", self._current_segment.get("funnel_source_include", [])),
                "funnel_source_exclude": self.generate_sql_list_filter("funnel_source", self._current_segment.get("funnel_source_exclude", []), exclude=True),
                "variations": [variation + 1 for variation in range(exp_info["variations"])],
                "platform_suffix": "web" if exp_info["calc_source"] == "UG_WEB" else "app"
            })


    def generate_sql_rights_filter(self, rights_type: str, rights: str):
        rights_level_list = ['pro', 'edu', 'sing', 'practice', 'book']
        rights_level = int(math.pow(10, rights_level_list.index(rights_type)))
        rights_dict: dict = {
            'empty': f'toUInt32(rights / {rights_level}) % 10 = 0',
            'free': f'toUInt32(rights / {rights_level}) % 10 in (0, 4, 5)',
            'finite subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2)',
            'lifetime': f'toUInt32(rights / {rights_level}) % 10 in (3)',
            'any paid': f'toUInt32(rights / {rights_level}) % 10 in (2, 3)',
            'any subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2, 3)',
            'trial': f'toUInt32(rights / {rights_level}) % 10 in (1)',
            'expired subscription': f'toUInt32(rights / {rights_level}) % 10 in (5)',
            'expired trial': f'toUInt32(rights / {rights_level}) % 10 in (4)',
            'expired any': f'toUInt32(rights / {rights_level}) % 10 in (4, 5)',
            'all': f'1'
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
            # query = self.get_query("get_exp_info", params=dict({"id": 6659}))
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
        # {self.get_query(f"retention_{platform_suffix}")}
        # {self.get_query(f"retention_mob_web")}
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
        # {self.get_query(f"long_tab_view_{platform_suffix}")}
        # {self.get_query(f"long_tab_view_mob_web")}
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


    def build_dau_query(self, platform_suffix: str):
        query = self.get_query(f"dau_{platform_suffix}")
        # print(query)
        return query


    def  build_custom_funnel_query(self, dag, platform_suffix):
        # dag = params.get("dag", "")
        query = f"""
            with vars as (
                {self.get_query("date_variation")}
            ),
            members as (
                {self.get_query(f"members_{platform_suffix}")}
            ),
            {generate_clickhouse_sql(dag)}
            select * from vars left join funnel using(dt, variation)
        """
        return query


    def get_exp_daily_monetization_data(self, params: dict = {}):
        monetization_query = self.build_monetization_query(params["platform_suffix"], params["ex_subs_filter"]).format(**params)
        print(params["platform_suffix"])
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


    def get_exp_daily_custom_funnel_data(self, params: dict = {}):
        custom_funnel_query = self.build_custom_funnel_query(params["dag"], params["platform_suffix"]).format(**params)
        # print(custom_funnel_query)
        query_result = self._mb_client.post("dataset", custom_funnel_query)
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
            params["ex_subs_filter"] = ""
            if exp_info["calc_source"].lower() == "ug_web" and (2 in exp_info.get("calc_platforms", [1]) or 3 in exp_info.get("calc_platforms", [1])):
                params["platform_suffix"] = "mob_web"
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

            if exp_info["calc_source"].lower() == "ug_web" and (2 in exp_info.get("calc_platforms", [1]) or 3 in exp_info.get("calc_platforms", [1])):
                params["platform_suffix"] = "mob_web"
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
            if exp_info["calc_source"].lower() == "ug_web" and (2 in exp_info.get("calc_platforms", [1]) or 3 in exp_info.get("calc_platforms", [1])):
                params["platform_suffix"] = "mob_web"
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
            df = self._mb_client.post("dataset", self.build_dau_query(params["platform_suffix"]).format(**params))
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df


    def get_custom_funnel_data(self, exp_info, funnel) -> pd.DataFrame:
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
            params["dag"] = funnel
            # params["platform_suffix"] = exp_info.get("platform_suffix", "app")
            # print("platform_suffix=", params["platform_suffix"])
            params["table"] = "default.ug_rt_events_app" if params["platform_suffix"] == "app" else "default.ug_rt_events_web"
            # print(self.build_custom_funnel_query(params["dag"], params["platform_suffix"]).format(**params))
            df = self.get_exp_daily_custom_funnel_data(params)
            print("DAY", day)
            print(df)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df


    def get_all_experiments(self) -> pd.DataFrame:
        query = self.get_query("get_all_experiments")
        query_result = self._mb_client.post("dataset", query)
        df = query_result
        experiments_dict = {
            "id": [],
            "date_start": [],
            "date_end": [],
            "variations": [],
            "experiment_event_start": [],
            "configuration": [],
            "clients_list": [],
            "clients_options": [],
            "url": []
        }
        for index, row in df.iterrows():
            pattern = r'https://alice\.mu\.se[^\s#?"]*(?:\?[^\s#"]*)?'
            urls = re.findall(pattern, str(row["configuration"]))
            url = urls[0] if urls else ''
            if url == '':
                continue
            experiments_dict["id"].append(row["id"])
            experiments_dict["date_start"].append(row["date_start"])
            experiments_dict["date_end"].append(row["date_end"])
            experiments_dict["variations"].append(row["variations"])
            experiments_dict["experiment_event_start"].append(row["experiment_event_start"])
            experiments_dict["configuration"].append(row["configuration"])
            experiments_dict["clients_list"].append(re.findall(r'(\w+)', row["clients"]))
            experiments_dict["clients_options"].append(row["clients_options"])
            experiments_dict["url"].append(url)

        experiments_df = pd.DataFrame(experiments_dict)
        return experiments_df



# def test(d: dict = {}):
#     print(d)

# test({'a': 1, 'b': 2})
# test()