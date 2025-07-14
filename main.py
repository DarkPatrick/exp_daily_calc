from decimal import Decimal
import pandas as pd
from sql_worker import SqlWorker
import re
import random
import string
import datetime
import os
from tqdm import tqdm

from metric_calculator import calc_monetization_cumulatives, calc_retention_cumulatives
from df_processing import DF_Processor
from stats import Stats
from plot_builder import PlotBuilder
from confluence import ConfluenceWorker
from exp_results_generator import ExpResultsGenerator
from html_generator import HTMLGenerator



def generate_metric_color(value, diff, is_positive=True):
    if value >= 0.05:
        return 'class="highlight-#fffae6 confluenceTd" data-highlight-colour="#fffae6" bgcolor="#fffae6"'
    elif is_positive and diff > 0 or not is_positive and diff < 0:
        return 'class="highlight-#e3fcef confluenceTd" data-highlight-colour="#e3fcef" bgcolor="#e3fcef"'
    else:
        return 'class="highlight-#ffebe6 confluenceTd" data-highlight-colour="#ffebe6" bgcolor="#ffebe6"'


def pvalue_round(number: float, alpha: float=0.05, min_val: float=1e-4) -> float:
    if number <= min_val:
        return "0.00"
    elif number <= alpha:
        return f"{number:.3f}"
    else:
        return f"{number:.2f}"

def generate_image_markup(image_file_name, width=250, height=250):
        image_markup = f'<ac:image ac:width="{width}" ac:height="{height}"><ri:attachment ri:filename="{image_file_name}" ri:version-at-save="1" /></ac:image>'
        return image_markup


def generate_random_id(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


calc_session = generate_random_id(128)

sql_worker: SqlWorker = SqlWorker()
exp_results_gen = ExpResultsGenerator(sql_worker, 6128)
exp_results = exp_results_gen.get_exp_all_calculations()

# import sys
# sys.exit()

# exp_5802_info: dict = sql_worker.get_experiment(6191)
# exp_5802_info: dict = sql_worker.get_experiment(6128)
# print(exp_5802_info)

# exp_data_df = sql_worker.get_exp_monetization_data(exp_5802_info)
# print(exp_data_df)
# print(exp_data_df.prices_per_buyer[1])
# print(exp_data_df)

# retention_data_df = sql_worker.get_exp_retention_data(exp_5802_info)
# print(retention_data_df)
# retention_result_df = calc_retention_cumulatives(retention_data_df)
# retention_result_df.to_csv("retention_result.csv", index=False)
# retention_df_processor = DF_Processor("retention_result.csv", "metrics.yaml")
# stats = Stats()
# result_df, stat_results_df = stats.evaluate_metrics(retention_df_processor)
# result_df.to_csv("result_metrics.csv", index=False)
# stat_results_df.to_csv("stat_results.csv", index=False)

# cum_result_df = stats.create_summary_table(result_df)
# cum_stat_results_df = stats.create_summary_table(stat_results_df, stats=True)
# cum_result_df.to_csv("cum_result.csv", index=False)
# cum_stat_results_df.to_csv("cum_stat_results.csv", index=False)


# result = calc_monetization_cumulatives(exp_data_df)
# # save result to csv without index
# result.to_csv("result.csv", index=False)

# df_processor = DF_Processor("result.csv", "metrics.yaml")
# stats = Stats()
# result_df, stat_results_df = stats.evaluate_metrics(df_processor)
# result_df.to_csv("result_metrics.csv", index=False)
# stat_results_df.to_csv("stat_results.csv", index=False)

# cum_result_df = stats.create_summary_table(result_df)
# cum_stat_results_df = stats.create_summary_table(stat_results_df, stats=True)
# cum_result_df.to_csv("cum_result.csv", index=False)
# cum_stat_results_df.to_csv("cum_stat_results.csv", index=False)


# plot_builder = PlotBuilder("plots/")
# plot_builder.save_plots(result_df)

# full_html_content = ''
# htm_rows = ''
# for id in range(len(cum_stat_results_df.index)):
#     rows_dict: dict = {
#         'variation': cum_stat_results_df.index[id],
#         'members': int(cum_stat_results_df['members'].iloc[id]),
#         'subscribers': int(cum_stat_results_df['subscribers'].iloc[id]),
#         'accesses': int(cum_stat_results_df['accesses'].iloc[id]),
#         'instants': int(cum_stat_results_df['instants'].iloc[id]),
#         'trials': int(cum_stat_results_df['trials'].iloc[id]),
#         'ex_trials': int(cum_stat_results_df['ex trials'].iloc[id]),
#         'charged_trials': int(cum_stat_results_df['charged trials'].iloc[id]),
#         'buyers': int(cum_stat_results_df['buyers'].iloc[id]),
#         'charges': int(cum_stat_results_df['charges'].iloc[id]),
#         'revenue': f"${int(cum_stat_results_df['revenue'].iloc[id])}",
#         'cancels_14d': int(cum_stat_results_df['cancel 14d'].iloc[id]),
#         'refunds_14d': int(cum_stat_results_df['refund 14d'].iloc[id])
#     }
#     with open(f"html/app_monetization_stats_row.html", 'r') as file:
#         html_content = file.read().format(
#             **rows_dict
#         )
#         htm_rows += html_content + '\n'

# with open(f"html/app_monetization_stats_header.html", 'r') as file:
#     html_content = file.read().format(rows=htm_rows)
# full_html_content += html_content

# # add html empty line
# full_html_content += '<br><br>'

# htm_rows = ''
# for id in range(len(cum_result_df.index)):
#     if cum_result_df.index[id] == 'pvalue':
#         rows_dict: dict = {
#             'arpu_color':  generate_metric_color(cum_result_df['arpu'].iloc[id], cum_result_df['arpu'].iloc[id - 1]),
#             'aov_color': generate_metric_color(cum_result_df['aov'].iloc[id], cum_result_df['aov'].iloc[id - 1]),
#             'arppu_color': generate_metric_color(cum_result_df['arppu'].iloc[id], cum_result_df['arppu'].iloc[id - 1]),
#             'access cr, %_color': generate_metric_color(cum_result_df['access cr, %'].iloc[id], cum_result_df['access cr, %'].iloc[id - 1]),
#             'charge cr, %_color': generate_metric_color(cum_result_df['charge cr, %'].iloc[id], cum_result_df['charge cr, %'].iloc[id - 1]),
#             'trial -> charge, %_color': generate_metric_color(cum_result_df['trial -> charge, %'].iloc[id], cum_result_df['trial -> charge, %'].iloc[id - 1]),
#             'charge -> 14d cancel, %_color': generate_metric_color(cum_result_df['charge -> 14d cancel, %'].iloc[id], cum_result_df['charge -> 14d cancel, %'].iloc[id - 1], False),
#             'charge -> 14d refund, %_color': generate_metric_color(cum_result_df['charge -> 14d refund, %'].iloc[id], cum_result_df['charge -> 14d refund, %'].iloc[id - 1], False),
#             'variation': cum_result_df.index[id],
#             'arpu': f"{pvalue_round(cum_result_df['arpu'].iloc[id])}",
#             'aov': f"{pvalue_round(cum_result_df['aov'].iloc[id])}",
#             'arppu': f"{pvalue_round(cum_result_df['arppu'].iloc[id])}",
#             'access cr, %': f"{pvalue_round(cum_result_df['access cr, %'].iloc[id])}",
#             'charge cr, %': f"{pvalue_round(cum_result_df['charge cr, %'].iloc[id])}",
#             'trial -> charge, %': f"{pvalue_round(cum_result_df['trial -> charge, %'].iloc[id])}",
#             'charge -> 14d cancel, %': f"{pvalue_round(cum_result_df['charge -> 14d cancel, %'].iloc[id])}",
#             'charge -> 14d refund, %': f"{pvalue_round(cum_result_df['charge -> 14d refund, %'].iloc[id])}"
#         }
#     elif cum_result_df.index[id] == 'cumulatives':
#         rows_dict: dict = {
#             'arpu_color': '',
#             'aov_color': '',
#             'arppu_color': '',
#             'access cr, %_color': '',
#             'charge cr, %_color': '',
#             'trial -> charge, %_color': '',
#             'charge -> 14d cancel, %_color': '',
#             'charge -> 14d refund, %_color': '',
#             'variation': cum_result_df.index[id],
#             'arpu': generate_image_markup(f'arpu_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'aov': generate_image_markup(f'aov_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'arppu': generate_image_markup(f'arppu_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'access cr, %': generate_image_markup(f'access cr, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'charge cr, %': generate_image_markup(f'charge cr, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'trial -> charge, %': generate_image_markup(f'trial -> charge, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'charge -> 14d cancel, %': generate_image_markup(f'charge -> 14d cancel, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
#             'charge -> 14d refund, %': generate_image_markup(f'charge -> 14d refund, %_pvalues_diff_confidence_intervals_{calc_session}.png')
#         }
#     else:
#         money_prefix = '$'
#         money_suffix = ''
#         if cum_result_df.index[id] == 'diff, %':
#             money_prefix = ''
#             money_suffix = '%'
#         rows_dict: dict = {
#             'arpu_color': '',
#             'aov_color': '',
#             'arppu_color': '',
#             'access cr, %_color': '',
#             'charge cr, %_color': '',
#             'trial -> charge, %_color': '',
#             'charge -> 14d cancel, %_color': '',
#             'charge -> 14d refund, %_color': '',
#             'variation': cum_result_df.index[id],
#             'arpu': f"""{money_prefix}{Decimal(f"{cum_result_df['arpu'].iloc[id]:.3g}"):f}{money_suffix}""",
#             'aov': f"""{money_prefix}{Decimal(f"{cum_result_df['aov'].iloc[id]:.3g}"):f}{money_suffix}""",
#             'arppu': f"""{money_prefix}{Decimal(f"{cum_result_df['arppu'].iloc[id]:.3g}"):f}{money_suffix}""",
#             'access cr, %': f"""{Decimal(f"{cum_result_df['access cr, %'].iloc[id]:.3g}"):f}%""",
#             'charge cr, %': f"""{Decimal(f"{cum_result_df['charge cr, %'].iloc[id]:.3g}"):f}%""",
#             'trial -> charge, %': f"""{Decimal(f"{cum_result_df['trial -> charge, %'].iloc[id]:.3g}"):f}%""",
#             'charge -> 14d cancel, %': f"""{Decimal(f"{cum_result_df['charge -> 14d cancel, %'].iloc[id]:.3g}"):f}%""",
#             'charge -> 14d refund, %': f"""{Decimal(f"{cum_result_df['charge -> 14d refund, %'].iloc[id]:.3g}"):f}%"""
#         }
    
#     with open(f"html/app_monetization_metrics_row.html", 'r') as file:
#         html_content = file.read().format(
#             **rows_dict
#         )
#         htm_rows += html_content + '\n'
# with open(f"html/app_monetization_metrics_header.html", 'r') as file:
#     html_content = file.read().format(rows=htm_rows)
# full_html_content += html_content

html_generator = HTMLGenerator()

full_html_content = ''
full_html_content += html_generator.generate_html(exp_results['monetization']['cum_stats'], 'app_monetization_stats', calc_session)
full_html_content += html_generator.generate_html(exp_results['monetization']['cum_metrics'], 'app_monetization_metrics', calc_session)
full_html_content += html_generator.generate_html(exp_results['retention']['cum_stats'], 'retention_stats', calc_session)
full_html_content += html_generator.generate_html(exp_results['retention']['cum_metrics'], 'retention_metrics', calc_session)
# print(html_generator.generate_html(exp_results['retention']['cum_metrics'], 'retention_metrics', calc_session))
print(full_html_content)


pattern = r'https://alice\.mu\.se[^\s#?"]*(?:\?[^\s#"]*)?'
urls = re.findall(pattern, exp_results_gen.exp_info["configuration"])
if not urls:
    url = ''
else:
    url = urls[0]
print(url)


confluence = ConfluenceWorker()

page_info = confluence.get_page_info(url)
page_id = page_info['page_id']
plot_dir = f"plots/exp_{exp_results_gen.exp_info['id']}/"
list_dir = os.listdir(plot_dir)
number_files = len(list_dir)
file_num = 1
                
for plot_file in tqdm(list_dir):
    confluence.upload_image(
        f'{plot_dir}{plot_file}',
        f'{os.path.splitext(plot_file)[0]}_{calc_session}.png', page_id)
    file_num += 1

# confluence.replace_expand_section(url, f"#{str(exp_5802_info['id'])}", full_html_content)
confluence.replace_expand_section(url, f"#{str(exp_results_gen.exp_info['id'])}", full_html_content)


