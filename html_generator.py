from typing import Dict
import pandas as pd
import numpy as np
from decimal import Decimal
from jinja2 import Environment, FileSystemLoader



class HTMLGenerator:
    def __init__(self, template_dir: str = 'html/') -> None:
        self.template_dir = template_dir
    
    def generate_metric_color(self, value, diff, is_positive=True):
        if value >= 0.05:
            return 'class="highlight-#fffae6 confluenceTd" data-highlight-colour="#fffae6" bgcolor="#fffae6"'
        elif is_positive and diff > 0 or not is_positive and diff < 0:
            return 'class="highlight-#e3fcef confluenceTd" data-highlight-colour="#e3fcef" bgcolor="#e3fcef"'
        else:
            return 'class="highlight-#ffebe6 confluenceTd" data-highlight-colour="#ffebe6" bgcolor="#ffebe6"'


    def pvalue_round(self, number: float, alpha: float=0.05, min_val: float=1e-4) -> float:
        if number <= min_val:
            return "0.00"
        elif number <= alpha:
            return f"{number:.3f}"
        else:
            return f"{number:.2f}"

    def generate_image_markup(self, image_file_name, width=250, height=250):
            image_markup = f'<ac:image ac:width="{width}" ac:height="{height}"><ri:attachment ri:filename="{image_file_name}" ri:version-at-save="1" /></ac:image>'
            return image_markup

    def generate_htm_dict(self, df: pd.DataFrame, id: int, template_name: str, calc_session: str) -> Dict:
        if template_name == 'app_monetization_stats':
            if df.index[id] == 'diff, %':
                rows_dict: dict = {
                    'variation': df.index[id],
                    'members': f"""{Decimal(f"{df['members'].iloc[id]:.2g}"):f}%""",
                    'subscribers': f"""{Decimal(f"{df['subscribers'].iloc[id]:.2g}"):f}%""",
                    'accesses': f"""{Decimal(f"{df['accesses'].iloc[id]:.2g}"):f}%""",
                    'instants': f"""{Decimal(f"{df['instants'].iloc[id]:.2g}"):f}%""",
                    'trials': f"""{Decimal(f"{df['trials'].iloc[id]:.2g}"):f}%""",
                    'ex_trials': f"""{Decimal(f"{df['ex trials'].iloc[id]:.2g}"):f}%""",
                    'charged_trials': f"""{Decimal(f"{df['charged trials'].iloc[id]:.2g}"):f}%""",
                    'buyers': f"""{Decimal(f"{df['buyers'].iloc[id]:.2g}"):f}%""",
                    'charges': f"""{Decimal(f"{df['charges'].iloc[id]:.2g}"):f}%""",
                    'revenue': f"""{Decimal(f"{df['revenue'].iloc[id]:.2g}"):f}%""",
                    'cancels_14d': f"""{Decimal(f"{df['cancel 14d'].iloc[id]:.2g}"):f}%""",
                    'refunds_14d': f"""{Decimal(f"{df['refund 14d'].iloc[id]:.2g}"):f}%"""
                }
            else:   
                rows_dict: dict = {
                    'variation': df.index[id],
                    'members': int(df['members'].iloc[id]),
                    'subscribers': int(df['subscribers'].iloc[id]),
                    'accesses': int(df['accesses'].iloc[id]),
                    'instants': int(df['instants'].iloc[id]),
                    'trials': int(df['trials'].iloc[id]),
                    'ex_trials': int(df['ex trials'].iloc[id]),
                    'charged_trials': int(df['charged trials'].iloc[id]),
                    'buyers': int(df['buyers'].iloc[id]),
                    'charges': int(df['charges'].iloc[id]),
                    'revenue': f"${int(df['revenue'].iloc[id])}",
                    'cancels_14d': int(df['cancel 14d'].iloc[id]),
                    'refunds_14d': int(df['refund 14d'].iloc[id])
                }
            return rows_dict
        elif template_name == 'app_monetization_metrics':
            if df.index[id] == 'pvalue':
                rows_dict: dict = {
                    'arpu_color':  self.generate_metric_color(df['arpu'].iloc[id], df['arpu'].iloc[id - 1]),
                    'aov_color': self.generate_metric_color(df['aov'].iloc[id], df['aov'].iloc[id - 1]),
                    'arppu_color': self.generate_metric_color(df['arppu'].iloc[id], df['arppu'].iloc[id - 1]),
                    'access cr, %_color': self.generate_metric_color(df['access cr, %'].iloc[id], df['access cr, %'].iloc[id - 1]),
                    'charge cr, %_color': self.generate_metric_color(df['charge cr, %'].iloc[id], df['charge cr, %'].iloc[id - 1]),
                    'trial -> charge, %_color': self.generate_metric_color(df['trial -> charge, %'].iloc[id], df['trial -> charge, %'].iloc[id - 1]),
                    'charge -> 14d cancel, %_color': self.generate_metric_color(df['charge -> 14d cancel, %'].iloc[id], df['charge -> 14d cancel, %'].iloc[id - 1], False),
                    'charge -> 14d refund, %_color': self.generate_metric_color(df['charge -> 14d refund, %'].iloc[id], df['charge -> 14d refund, %'].iloc[id - 1], False),
                    'variation': df.index[id],
                    'arpu': f"{self.pvalue_round(df['arpu'].iloc[id])}",
                    'aov': f"{self.pvalue_round(df['aov'].iloc[id])}",
                    'arppu': f"{self.pvalue_round(df['arppu'].iloc[id])}",
                    'access cr, %': f"{self.pvalue_round(df['access cr, %'].iloc[id])}",
                    'charge cr, %': f"{self.pvalue_round(df['charge cr, %'].iloc[id])}",
                    'trial -> charge, %': f"{self.pvalue_round(df['trial -> charge, %'].iloc[id])}",
                    'charge -> 14d cancel, %': f"{self.pvalue_round(df['charge -> 14d cancel, %'].iloc[id])}",
                    'charge -> 14d refund, %': f"{self.pvalue_round(df['charge -> 14d refund, %'].iloc[id])}"
                }
            elif df.index[id] == 'cumulatives':
                rows_dict: dict = {
                    'arpu_color': '',
                    'aov_color': '',
                    'arppu_color': '',
                    'access cr, %_color': '',
                    'charge cr, %_color': '',
                    'trial -> charge, %_color': '',
                    'charge -> 14d cancel, %_color': '',
                    'charge -> 14d refund, %_color': '',
                    'variation': df.index[id],
                    'arpu': self.generate_image_markup(f'arpu_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'aov': self.generate_image_markup(f'aov_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'arppu': self.generate_image_markup(f'arppu_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'access cr, %': self.generate_image_markup(f'access cr, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'charge cr, %': self.generate_image_markup(f'charge cr, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'trial -> charge, %': self.generate_image_markup(f'trial -> charge, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'charge -> 14d cancel, %': self.generate_image_markup(f'charge -> 14d cancel, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'charge -> 14d refund, %': self.generate_image_markup(f'charge -> 14d refund, %_pvalues_diff_confidence_intervals_{calc_session}.png')
                }
            else:
                money_prefix = '$'
                money_suffix = ''
                if df.index[id] == 'diff, %':
                    money_prefix = ''
                    money_suffix = '%'
                rows_dict: dict = {
                    'arpu_color': '',
                    'aov_color': '',
                    'arppu_color': '',
                    'access cr, %_color': '',
                    'charge cr, %_color': '',
                    'trial -> charge, %_color': '',
                    'charge -> 14d cancel, %_color': '',
                    'charge -> 14d refund, %_color': '',
                    'variation': df.index[id],
                    'arpu': f"""{money_prefix}{Decimal(f"{df['arpu'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'aov': f"""{money_prefix}{Decimal(f"{df['aov'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'arppu': f"""{money_prefix}{Decimal(f"{df['arppu'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'access cr, %': f"""{Decimal(f"{df['access cr, %'].iloc[id]:.3g}"):f}%""",
                    'charge cr, %': f"""{Decimal(f"{df['charge cr, %'].iloc[id]:.3g}"):f}%""",
                    'trial -> charge, %': f"""{Decimal(f"{df['trial -> charge, %'].iloc[id]:.3g}"):f}%""",
                    'charge -> 14d cancel, %': f"""{Decimal(f"{df['charge -> 14d cancel, %'].iloc[id]:.3g}"):f}%""",
                    'charge -> 14d refund, %': f"""{Decimal(f"{df['charge -> 14d refund, %'].iloc[id]:.3g}"):f}%"""
                }
            return rows_dict
        elif template_name == 'retention_stats':
            if df.index[id] == 'diff, %':
                rows_dict: dict = {
                    'variation': df.index[id],
                    'members': f"""{Decimal(f"{df['members'].iloc[id]:.2g}"):f}%""",
                    'retention 1d': f"""{Decimal(f"{df['retention 1d'].iloc[id]:.2g}"):f}%""",
                    'retention 7d': f"""{Decimal(f"{df['retention 7d'].iloc[id]:.2g}"):f}%""",
                    'retention 14d': f"""{Decimal(f"{df['retention 14d'].iloc[id]:.2g}"):f}%"""
                }
            else:
                rows_dict: dict = {
                    'variation': df.index[id],
                    'members': int(df['members'].iloc[id]),
                    'retention 1d': int(df['retention 1d'].iloc[id]),
                    'retention 7d': int(df['retention 7d'].iloc[id]),
                    'retention 14d': int(df['retention 14d'].iloc[id])
                }
            return rows_dict
        elif template_name == 'retention_metrics':
            if df.index[id] == 'pvalue':
                rows_dict: dict = {
                    'retention 1d, %_color':  self.generate_metric_color(df['retention 1d, %'].iloc[id], df['retention 1d, %'].iloc[id - 1]),
                    'retention 7d, %_color':  self.generate_metric_color(df['retention 7d, %'].iloc[id], df['retention 7d, %'].iloc[id - 1]),
                    'retention 14d, %_color':  self.generate_metric_color(df['retention 14d, %'].iloc[id], df['retention 14d, %'].iloc[id - 1]),
                    'variation': df.index[id],
                    'retention 1d, %': f"{self.pvalue_round(df['retention 1d, %'].iloc[id])}",
                    'retention 7d, %': f"{self.pvalue_round(df['retention 7d, %'].iloc[id])}",
                    'retention 14d, %': f"{self.pvalue_round(df['retention 14d, %'].iloc[id])}"
                }
            elif df.index[id] == 'cumulatives':
                rows_dict: dict = {
                    'retention 1d, %_color': '',
                    'retention 7d, %_color': '',
                    'retention 14d, %_color': '',
                    'variation': df.index[id],
                    'retention 1d, %': self.generate_image_markup(f'retention 1d, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'retention 7d, %': self.generate_image_markup(f'retention 7d, %_pvalues_diff_confidence_intervals_{calc_session}.png'),
                    'retention 14d, %': self.generate_image_markup(f'retention 14d, %_pvalues_diff_confidence_intervals_{calc_session}.png')
                }
            else:
                money_prefix = '$'
                money_suffix = ''
                if df.index[id] == 'diff, %':
                    money_prefix = ''
                    money_suffix = '%'
                rows_dict: dict = {
                    'retention 1d, %_color': '',
                    'retention 7d, %_color': '',
                    'retention 14d, %_color': '',
                    'variation': df.index[id],
                    'retention 1d, %': f"""{Decimal(f"{df['retention 1d, %'].iloc[id]:.3g}"):f}%""",
                    'retention 7d, %': f"""{Decimal(f"{df['retention 7d, %'].iloc[id]:.3g}"):f}%""",
                    'retention 14d, %': f"""{Decimal(f"{df['retention 14d, %'].iloc[id]:.3g}"):f}%"""
                }
            return rows_dict
        elif template_name == 'forecast':
            if df.index[id] == 'diff, %':
                rows_dict: dict = {
                    'variation': 'diff',
                    'members': f"""{Decimal(f"{df['members'].iloc[id]:.2g}"):f}%""",
                    'accesses': f"""{Decimal(f"{df['accesses'].iloc[id]:.2g}"):f}%""",
                    'charges': f"""{Decimal(f"{df['charges'].iloc[id]:.2g}"):f}%""",
                    'revenue': f"""{Decimal(f"{df['revenue'].iloc[id]:.2g}"):f}%"""
                }
            else:   
                rows_dict: dict = {
                    'variation': df.index[id],
                    'members': int(df['members'].iloc[id]),
                    'accesses': int(df['accesses'].iloc[id]),
                    'charges': int(df['charges'].iloc[id]),
                    'revenue': f"${int(df['revenue'].iloc[id])}"
                }
            return rows_dict


    def generate_html_results_table(self, df: pd.DataFrame, template_name: str, calc_session: str) -> str:
        htm_rows = ''
        html_content = ''
        for id in range(len(df.index)):
            rows_dict: dict = self.generate_htm_dict(df, id, template_name, calc_session)
            with open(f"{self.template_dir}{template_name}_row.html", 'r') as file:
                html_content = file.read().format(
                    **rows_dict
                )
                htm_rows += html_content + '\n'

        with open(f"{self.template_dir}{template_name}_header.html", 'r') as file:
            html_content = file.read().format(rows=htm_rows)

        html_content += """
        <p>
        &nbsp;
        <span>&nbsp;</span>
        </p>
        """

        return html_content

    def generate_exp_results_header(self, exp_info) -> str:
        # convert np.int64(1748854633) to datetime string
        date_start = pd.to_datetime(exp_info['date_start'], unit='s', utc=True)
        date_end = pd.to_datetime(exp_info['date_end'], unit='s', utc=True)
        # load the template
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('exp_header.html')
        html_content = template.render(
            exp_id=exp_info['id'],
            start_date=date_start.strftime('%Y-%m-%d'),
            end_date=date_end.strftime('%Y-%m-%d'),
            exposure_event=exp_info['experiment_event_start']
        )
        return html_content


    def generate_html_header_table(self, df: pd.DataFrame) -> str:
        platforms_list = ['iOS', 'Android']
        branches = ['A', 'B', 'C']

        platforms = []
        for plat in platforms_list:
            design_duration = np.random.randint(5, 11)
            experiment_duration = np.random.randint(3, 13)
            design_samples = {b: np.random.randint(100000, 200000) for b in branches}
            exp_samples    = {b: np.random.randint(80000, 200000)  for b in branches}
            platforms.append({
                'name': plat,
                'design':     {'duration': design_duration,   'samples': design_samples},
                'experiment': {'duration': experiment_duration, 'samples': exp_samples}
            })

        default_checks = {
            'no_visible_bugs': True,
            'no_external_effects': True
        }
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('design_vs_reality.html')
        html_content = template.render(platforms=platforms, branches=branches, default_checks=default_checks)
        # html_content = template.render()
        return html_content

    def generate_decision_section(self) -> str:
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('decision.html')
        html_content = template.render()
        return html_content

    def generate_forecast_section(self, exp_info: dict) -> str:
        env = Environment(loader=FileSystemLoader('templates'))
        template = env.get_template('forecast.html')
        html_content = template.render(exp_info=exp_info)
        return html_content
