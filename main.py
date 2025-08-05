# todo:
# историю экспов
# фильтр только монетизационные метрики
# воронка тура
# воронка регвола
# лтв 2 года
# сегменты через конфиг в доке вместо сторк с фильтрам хардкодить костаны: разбивка по трафику. доступы в туре / только сплешы / без сплешей / без тура
import pandas as pd
from sql_worker import SqlWorker
import re
import random
import string
import datetime
import os
from tqdm import tqdm

from confluence import ConfluenceWorker
from exp_results_generator import ExpResultsGenerator
from html_generator import HTMLGenerator



def generate_random_id(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


calc_session = generate_random_id(128)

sql_worker: SqlWorker = SqlWorker()

# exp_results_gen = ExpResultsGenerator(sql_worker, 6308)
# exp_results_gen = ExpResultsGenerator(sql_worker, 5592)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6260)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6293)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6305)


# exp_results_gen = ExpResultsGenerator(sql_worker, 5682)
# exp_results_gen = ExpResultsGenerator(sql_worker, 5685)
# exp_results_gen = ExpResultsGenerator(sql_worker, 5724)
# exp_results_gen = ExpResultsGenerator(sql_worker, 5748)


# exp_results_gen = ExpResultsGenerator(sql_worker, 5583)
# exp_results_gen = ExpResultsGenerator(sql_worker, 5960)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6050)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6152)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6245)


exp_results_gen = ExpResultsGenerator(sql_worker, 6404)


# exp_results_gen.exp_info
# for platform in exp_results_gen.exp_info['clients_list']:
#     print(platform)
# print(exp_results_gen.exp_info)

# exp_results_gen.exp_info['date_start'] = 1746809220
# exp_results_gen.exp_info['date_end'] = 1746985200


pattern = r'https://alice\.mu\.se[^\s#?"]*(?:\?[^\s#"]*)?'
urls = re.findall(pattern, exp_results_gen.exp_info["configuration"])
if not urls:
    url = ''
else:
    url = urls[0]
print(url)


confluence = ConfluenceWorker()

page_info = confluence.get_page_info(url)
# config_dict = confluence.parse_config_table(page_info['current_content'], exp_results_gen.exp_info['id'])
# print(config_dict)
# print(config_dict['total'])
# audience_dict = confluence.parse_audience_table(page_info['current_content'], exp_results_gen.exp_info['id'])
# print(audience_dict)
audience_dict = {'UGT_IOS': {'sample': 0, 'days': 0}, 'UGT_ANDROID': {'sample': 0, 'days': 0}}

# import sys
# sys.exit()

page_id = page_info['page_id']


clients_options = eval(exp_results_gen.exp_info['clients_options'])
exp_results = {}
for client in clients_options:
    # if client == 'UGT_ANDROID':
    #     exp_results_gen.exp_info['date_end'] = 1752482280
    # else:
    #     exp_results_gen.exp_info['date_end'] = 1753086660

    exp_results_gen.exp_info['calc_source'] = client
    for params in clients_options[client]:
        if params[0] == 'platform':
            exp_results_gen.exp_info['calc_platforms'] = params[1]
        if params[0] == 'version':
            exp_results_gen.exp_info['calc_version'] = params[1]
    exp_results_gen.exp_info['calc_source'] = client
    
    exp_results[client] = exp_results_gen.get_exp_all_calculations()

import sys
sys.exit()

html_generator = HTMLGenerator()

full_html_content = ''
full_html_content += html_generator.generate_exp_results_header(exp_results_gen.exp_info)
full_html_content += html_generator.generate_html_header_table(exp_results, audience_dict)
full_html_content += html_generator.generate_decision_section()

full_html_content += html_generator.generate_forecast_section(exp_results_gen.exp_info, exp_results)

# import sys
# sys.exit()
full_html_content += '\n\n<h2>Significance analysis</h2>\n\n'

for client in clients_options:
    full_html_content += f"""
    \n\n
    <ac:structured-macro ac:name="ui-expand" ac:macro-id="{exp_results_gen.exp_info['id']}_{client}">
    <ac:parameter ac:name="title">{client}</ac:parameter>
    <ac:rich-text-body>
    """
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['monetization']['cum_stats'], 'app_monetization_stats', f'{calc_session}_{client}')
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['retention']['cum_stats'], 'retention_stats', f'{calc_session}_{client}')
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['long_tab_view']['cum_stats'], 'long_tab_view_stats', f'{calc_session}_{client}')
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['monetization']['cum_metrics'], 'app_monetization_metrics', f'{calc_session}_{client}')
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['retention']['cum_metrics'], 'retention_metrics', f'{calc_session}_{client}')
    full_html_content += html_generator.generate_html_results_table(exp_results[client]['long_tab_view']['cum_metrics'], 'long_tab_view_metrics', f'{calc_session}_{client}')
    full_html_content += f"""
    \n\n
    </ac:rich-text-body>
    </ac:structured-macro>
    \n\n
    """

# import sys
# sys.exit()


for client in clients_options:    
    plot_dir = f"plots/exp_{exp_results_gen.exp_info['id']}_{client}/"
    list_dir = os.listdir(plot_dir)
    number_files = len(list_dir)
    file_num = 1

    for plot_file in tqdm(list_dir):
        confluence.upload_image(
            f'{plot_dir}{plot_file}',
            f'{os.path.splitext(plot_file)[0]}_{calc_session}_{client}.png', page_id)
        file_num += 1

confluence.replace_expand_section(url, f"#{str(exp_results_gen.exp_info['id'])}", full_html_content)


