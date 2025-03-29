import pandas as pd
from sql_worker import SqlWorker


sql_worker: SqlWorker = SqlWorker()
exp_5873_info: dict = sql_worker.get_experiment(5873)
print(exp_5873_info)

exp_data_df = sql_worker.get_exp_data(exp_5873_info)
exp_data_df
print(exp_data_df)

