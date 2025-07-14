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



def generate_random_id(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


calc_session = generate_random_id(128)

sql_worker: SqlWorker = SqlWorker()
exp_results_gen = ExpResultsGenerator(sql_worker, 6128)
exp_results = exp_results_gen.get_exp_all_calculations()


html_generator = HTMLGenerator()

full_html_content = ''
full_html_content += html_generator.generate_html(exp_results['monetization']['cum_stats'], 'app_monetization_stats', calc_session)
full_html_content += html_generator.generate_html(exp_results['monetization']['cum_metrics'], 'app_monetization_metrics', calc_session)
full_html_content += html_generator.generate_html(exp_results['retention']['cum_stats'], 'retention_stats', calc_session)
full_html_content += html_generator.generate_html(exp_results['retention']['cum_metrics'], 'retention_metrics', calc_session)


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


