from typing import Dict
import pandas as pd
import os

from df_processing import DF_Processor
from metric_calculator import (
    calc_monetization_cumulatives,
    calc_retention_cumulatives
)
from plot_builder import PlotBuilder
from sql_worker import SqlWorker
from stats import Stats



class ExpResultsGenerator:
    def __init__(self, sql_worker: SqlWorker, experiment_id: int) -> None:
        self.db: SqlWorker = sql_worker
        self.experiment_id: int = experiment_id
        self.exp_info: dict = sql_worker.get_experiment(self.experiment_id)
        if not os.path.exists(f"exp_results/exp_{experiment_id}"):
            os.makedirs(f"exp_results/exp_{experiment_id}")
        if not os.path.exists(f"plots/exp_{experiment_id}"):
            os.makedirs(f"plots/exp_{experiment_id}")
        self.results_path = f"exp_results/exp_{experiment_id}/"
        self.plot_builder = PlotBuilder(f"plots/exp_{experiment_id}/")


    def generate_cum_files(self) -> Dict[str, pd.DataFrame]:
        monetization_data_df = self.db.get_exp_monetization_data(self.exp_info)
        retention_data_df = self.db.get_exp_retention_data(self.exp_info)

        monetization_results_df = calc_monetization_cumulatives(monetization_data_df)
        retention_result_df = calc_retention_cumulatives(retention_data_df)

        monetization_results_df.to_csv(f"{self.results_path}monetization_result.csv", index=False)
        retention_result_df.to_csv(f"{self.results_path}retention_result.csv", index=False)

        return {
            'monetization': monetization_results_df,
            'retention': retention_result_df
        }


    def generate_results_dfs(self, results_file: str, suffix: str) -> Dict[str, pd.DataFrame]:
        retention_df_processor = DF_Processor(results_file, "metrics.yaml")
        stats = Stats()

        result_df, stat_results_df = stats.evaluate_metrics(retention_df_processor)
        result_df.to_csv(f"{self.results_path}result_metrics_{suffix}.csv", index=False)
        stat_results_df.to_csv(f"{self.results_path}stat_results_{suffix}.csv", index=False)
        cum_result_df = stats.create_summary_table(result_df)
        cum_stat_results_df = stats.create_summary_table(stat_results_df, stats=True)
        cum_result_df.to_csv(f"{self.results_path}cum_result_{suffix}.csv", index=False)
        cum_stat_results_df.to_csv(f"{self.results_path}cum_stat_results_{suffix}.csv", index=False)

        return {
            'metrics': result_df,
            'stats': stat_results_df,
            'cum_metrics': cum_result_df,
            'cum_stats': cum_stat_results_df
        }


    def get_exp_all_calculations(self):
        self.generate_cum_files()
        monetization_res = self.generate_results_dfs(f'{self.results_path}monetization_result.csv', 'monetization')
        retention_res = self.generate_results_dfs(f'{self.results_path}retention_result.csv', 'retention')
        self.plot_builder.save_plots(monetization_res['metrics'])
        self.plot_builder.save_plots(retention_res['metrics'])
        return {
            'monetization': monetization_res,
            'retention': retention_res
        }