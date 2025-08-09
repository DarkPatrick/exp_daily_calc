# todo:
# историю экспов
# фильтр только монетизационные метрики
# воронка тура
# воронка регвола
# лтв 2 года
# сегменты через конфиг в доке вместо сторк с фильтрам хардкодить костаны: разбивка по трафику. доступы в туре / только сплешы / без сплешей / без тура
# trials share починить график
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

# def update_experiments_info():
#     experiments_df = sql_worker.get_all_experiments()
#     return experiments_df

# print(update_experiments_info())

# import sys
# sys.exit()

exp_results_gen = ExpResultsGenerator(sql_worker, 6335)


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
config_dict_raw = confluence.parse_config_table(page_info['current_content'], exp_results_gen.exp_info['id'])
config_dict = {}
funnels = {}
if config_dict_raw == {}:
    config_dict  = {'Total': {'pro_rights': 'All'}}
else:
    for segment, config in config_dict_raw.items():
        if 'funnel' not in config_dict_raw[segment]:
            config_dict[segment] = config_dict_raw[segment] 
        else:
            funnels[segment] = config_dict_raw[segment]['funnel']
# print(config_dict_raw)
# print(config_dict)
# print(config_dict['Total'])
audience_dict = confluence.parse_audience_table(page_info['current_content'], exp_results_gen.exp_info['id'])
if audience_dict == {}:
    audience_dict = {'UGT_IOS': {'sample': 0, 'days': 0}, 'UGT_ANDROID': {'sample': 0, 'days': 0}, 'UG_WEB': {'sample': 0, 'days': 0}}
# print(audience_dict)
# print(funnels)
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
    exp_results[client] = {}

    for segment in config_dict:
        for params in clients_options[client]:
            if params[0] == 'platform':
                exp_results_gen.exp_info['calc_platforms'] = params[1]
            if params[0] == 'version':
                exp_results_gen.exp_info['calc_version'] = params[1]
        exp_results_gen.exp_info['segment'] = segment
        exp_results_gen.db._current_segment = config_dict[segment]
        exp_results_gen.db._funnels = funnels

        exp_results[client][segment] = exp_results_gen.get_exp_all_calculations()

# import sys
# sys.exit()

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
    full_html_content += html_generator.generate_html_results_table(
        {segment: exp_results[client][segment]['monetization']['cum_stats'] for segment in config_dict}, 
        'app_monetization_stats', [f'{calc_session}_{client}_{segment}' for segment in config_dict])
    full_html_content += html_generator.generate_html_results_table({'Total': exp_results[client]['Total']['retention']['cum_stats']}, 'retention_stats', [f'{calc_session}_{client}_Total'])
    full_html_content += html_generator.generate_html_results_table({'Total': exp_results[client]['Total']['long_tab_view']['cum_stats']}, 'long_tab_view_stats', [f'{calc_session}_{client}_Total'])
    full_html_content += html_generator.generate_html_results_table(
        {segment: exp_results[client][segment]['monetization']['cum_metrics'] for segment in config_dict}, 
        'app_monetization_metrics', [f'{calc_session}_{client}_{segment}' for segment in config_dict])
    full_html_content += html_generator.generate_html_results_table({'Total': exp_results[client]['Total']['retention']['cum_metrics']}, 'retention_metrics', [f'{calc_session}_{client}_Total'])
    full_html_content += html_generator.generate_html_results_table({'Total': exp_results[client]['Total']['long_tab_view']['cum_metrics']}, 'long_tab_view_metrics', [f'{calc_session}_{client}_Total'])
    if funnels:
        for funnel_name, funnel_data in funnels.items():
            if funnel_name in exp_results[client]['Total']['funnel_data']:
                full_html_content += html_generator.generate_custom_funnel_section(
                    exp_results[client]['Total']['funnel_data'][funnel_name], 
                    funnel_name)
        
    full_html_content += f"""
    \n\n
    </ac:rich-text-body>
    </ac:structured-macro>
    \n\n
    """

# print(full_html_content)

# import sys
# sys.exit()


for client in clients_options:
    for segment in config_dict:
        plot_dir = f"plots/exp_{exp_results_gen.exp_info['id']}_{client}_{segment}/"
        list_dir = os.listdir(plot_dir)
        number_files = len(list_dir)
        file_num = 1

        for plot_file in tqdm(list_dir):
            confluence.upload_image(
                f'{plot_dir}{plot_file}',
                f'{os.path.splitext(plot_file)[0]}_{calc_session}_{client}_{segment}.png', page_id)
            file_num += 1

confluence.replace_expand_section(url, f"#{str(exp_results_gen.exp_info['id'])}", full_html_content)


