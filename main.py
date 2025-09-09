# todo:
# историю экспов
# фильтр только монетизационные метрики
# воронка тура
# воронка регвола
# лтв 2 года
# design check: надо поделить на количество веток
import sys
import pandas as pd
from sql_worker import SqlWorker
import re
import random
import string
import datetime
import os
from tqdm import tqdm
from requests.structures import CaseInsensitiveDict

from confluence import ConfluenceWorker
from exp_results_generator import ExpResultsGenerator
from html_generator import HTMLGenerator
from agent import generate_gpt_prompt, ask_gpt_opinion, gpt_advice_to_confluence_html



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

# exp_results_gen = ExpResultsGenerator(sql_worker, 6335)


# exp_results_gen = ExpResultsGenerator(sql_worker, 6374)


# exp_results_gen = ExpResultsGenerator(sql_worker, 6320)


# exp_results_gen = ExpResultsGenerator(sql_worker, 6359)

# exp_results_gen = ExpResultsGenerator(sql_worker, 4722)


# exp_results_gen = ExpResultsGenerator(sql_worker, 5238)


# exp_results_gen = ExpResultsGenerator(sql_worker, 3781)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6332)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6326)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6386)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6230)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6377)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6371)
# exp_results_gen = ExpResultsGenerator(sql_worker, 6398)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6413)


# exp_results_gen = ExpResultsGenerator(sql_worker, 6059)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6287)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6428)

# exp_results_gen = ExpResultsGenerator(sql_worker, 6344)

exp_results_gen = ExpResultsGenerator(sql_worker, 6440)


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
# config_dict_raw = {'Total': {'pro_rights': 'All', 'platform': 'all', 'include_values': ['Audio2Chords Tour Update'], 'funnel_source_include': ['Audio2Chords Tour Update']}}
if config_dict_raw == {} or config_dict_raw is None:
    config_dict  = {'Total': {'pro_rights': 'All'}}
    # config_dict  = {'Total': {'pro_rights': 'Free', 'Platform': 'Phone'}, 'Tour accesses only': {'pro_rights': 'Free', 'Platform': 'Phone', 'funnel_source_include': ['Tour Install']}}
    # config_dict  = {'Total': {'pro_rights': 'Free', 'platform': 'Mobile', 'country': 'Asia'}}
else:
    for segment, config in config_dict_raw.items():
        if 'funnel' not in config_dict_raw[segment]:
            config_dict[segment] = config_dict_raw[segment] 
        else:
            funnels[segment] = config_dict_raw[segment]['funnel']
print(config_dict_raw)
print(config_dict)
# print(config_dict['Total'])
audience_dict = confluence.parse_audience_table(page_info['current_content'], exp_results_gen.exp_info['id'])
if audience_dict == {}:
    audience_dict = {'UGT_IOS': {'sample': 0, 'days': 0}, 'UGT_ANDROID': {'sample': 0, 'days': 0}, 'UG_WEB': {'sample': 0, 'days': 0}}
print(audience_dict)
# print(audience_dict)
print(funnels)

solution = confluence.extract_solution_bullets(page_info['current_content'], exp_results_gen.exp_info['id'])
# solution = """
# If we enable Simplify feature be default in Asia countries, then Bounce rate metric will descrease, because chords will be easier so more users will be able to play a song thay want.
# """
print(solution)

# import sys
# sys.exit()

page_id = page_info['page_id']


clients_options = eval(exp_results_gen.exp_info['clients_options'])
print(clients_options)

# sys.exit()

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
                # print(exp_results_gen.exp_info['calc_platforms'])
                # sys.exit()
            if params[0] == 'version':
                exp_results_gen.exp_info['calc_version'] = params[1]
        exp_results_gen.exp_info['segment'] = segment
        exp_results_gen.db._current_segment = CaseInsensitiveDict(config_dict[segment])
        exp_results_gen.db._funnels = funnels
        if exp_results_gen.exp_info['calc_source'].lower() in ['ug_ios', 'ugt_ios', 'ug_android', 'ugt_android']:
            exp_results_gen.db._funnels['Tour Funnel'] = "members > event = 'Tour Start' > event = 'Tour General Goals View' > event = 'Banner Tour View' > event = 'Purchase Process Finish' and value = 'Tour Install' > event = 'Tour End'"
        if exp_results_gen.exp_info['calc_source'].lower() in ['ug_web']:
            exp_results_gen.db._funnels['Laning Funnel'] = "members > event = 'Landing Upgrade Open' > event = 'Landing Plans View' > event = 'Landing Checkout View' > event = 'Landing Purchase Click' > event = 'PURCHASE_SUCCESS'"

        exp_results[client][segment] = exp_results_gen.get_exp_all_calculations()


# sys.exit()

prompt = generate_gpt_prompt(clients_options, config_dict, exp_results, solution)
# print(prompt)
gpt_advice = ask_gpt_opinion(prompt)
# print(gpt_advice)

# import sys
# sys.exit()

html_generator = HTMLGenerator()

full_html_content = ''
full_html_content += html_generator.generate_exp_results_header(exp_results_gen.exp_info)
full_html_content += html_generator.generate_html_header_table(exp_results, audience_dict)
# full_html_content += html_generator.generate_decision_section()
# print(gpt_advice_to_confluence_html(gpt_advice))
full_html_content += gpt_advice_to_confluence_html(gpt_advice)

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

# save full_html_content to a htm file
# html_file_path = f"exp_results/exp_{exp_results_gen.exp_info['id']}_{calc_session}.htm"
# with open(html_file_path, 'w', encoding='utf-8') as html_file:
#     html_file.write(full_html_content)
# Upload HTML file to Confluence

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


