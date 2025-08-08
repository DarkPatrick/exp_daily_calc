from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values
import requests
import re
import json
from urllib.parse import urlparse, unquote
import base64
from bs4 import BeautifulSoup
import ast



class ConfluenceWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._base_url: str = secrets['confluence_url']
        self._api_token: str = secrets['confluence_api_token']

    def get_page_info(self, url):
        parsed_url = urlparse(url)
        page_id = parse_qs(parsed_url.query)['pageId'][0]
        search_url = f'{self._base_url}/rest/api/content/{page_id}?expand=body.storage,version'
        print(search_url)

        headers = {
            "Authorization": f"Bearer {self._api_token}",
            'Accept': 'application/json'
        }
        response = requests.get(search_url, headers=headers)
        if response.status_code == 200:
            results = response.json()
            if results:
                search_results = results
                results: dict = {
                    "page_id": page_id,
                    "page_version": search_results['version']["number"],
                    "page_title": search_results['title'],
                    "current_content": search_results['body']['storage']['value'],
                    "page_url": f'{self._base_url}/rest/api/content/{page_id}'
                }
                return results
            else:
                print("Page not found.")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
        return None


    def parse_config_table(self, html: str, id_value: int) -> dict:
        """
        Ищет в html таблицу с заголовком "#<id_value> config" и парсит её тело в dict:
        {
            segment1: {…conditions…},
            segment2: {…conditions…},
            …
        }
        """
        soup = BeautifulSoup(html, 'html.parser')
        target_header = f"#{id_value} config"

        # 1) Найти нужную таблицу по тексту в первом <th> первой строки <tbody>
        for tbl in soup.find_all('table'):
            tbody = tbl.find('tbody')
            if not tbody:
                continue
            rows = tbody.find_all('tr')
            if not rows:
                continue
            # смотрим на первый ряд
            first_cell = rows[0].find('th')
            if not first_cell or first_cell.get_text(strip=True) != target_header:
                continue

            # 2) Собираем результат, пропуская первые два ряда (заголовок конфигурации и заголовки колонок)
            result = {}
            for row in rows[2:]:
                tds = row.find_all('td')
                if len(tds) < 2:
                    continue  # неполная строка
                segment = tds[0].get_text(strip=True)
                cond_text = tds[1].get_text(strip=True)

                # 3) Парсим строку-словарь в настоящий dict
                try:
                    conditions = ast.literal_eval(cond_text)
                    if not isinstance(conditions, dict):
                        # raise ValueError
                        return {}
                except Exception:
                    # raise ValueError(f"Не удалось распарсить условия: {cond_text!r}")
                    return {}

                result[segment] = conditions

            return result

    def parse_audience_table(self, html: str, id_value: int) -> dict:
        """
        Ищет в html таблицу, в первой строке <tbody> которой есть ячейка с текстом "#{id_value} Audience",
        затем:
        1) Находит в этой таблице строку, где первый столбец == "Platform", и берёт имена платформ из оставшихся ячеек.
        2) Находит строки, где первый столбец == "Sample" и == "Days", и берёт из них числовые значения для каждой платформы.
        3) Собирает итоговый словарь вида:
            {
                platform1: {"sample": <int или str>, "days": <int или str>},
                platform2: {"sample": ... ,       "days": ...},
                ...
            }
        """
        soup = BeautifulSoup(html, 'html.parser')
        target_header = f"#{id_value} Audience"

        # 1) Найти таблицу по заголовку в первой строке
        table = None
        for tbl in soup.find_all('table'):
            tbody = tbl.find('thead')
            if not tbody:
                continue
            rows = tbody.find_all('tr')
            if not rows:
                continue
            # первая строка, первый столбец может быть <th> или <td>
            first_cell = rows[0].find(['th', 'td'])
            if first_cell and first_cell.get_text(strip=True) == target_header:
                table = tbl
                break

        if table is None:
            raise ValueError(f"Таблица с заголовком '{target_header}' не найдена")

        # 2) Собрать все строки из <tbody>
        rows = table.find('tbody').find_all('tr')

        # Вспомогательная функция: получить текст первого столбца строки
        def first_text(row):
            cell = row.find(['th', 'td'])
            return cell.get_text(strip=True) if cell else ""

        # Найти нужные строки
        platform_row = next((r for r in rows if first_text(r) == "Platform"), None)
        sample_row   = next((r for r in reversed(rows) if first_text(r) == "Sample"), None)
        days_row     = next((r for r in reversed(rows) if first_text(r) == "Days"), None)

        if not platform_row or not sample_row or not days_row:
            raise ValueError("Не найдены строки Platform, Sample или Days в таблице")

        # Извлечь списки ячеек (в т.ч. могут быть <td> или <th>)
        plat_cells   = platform_row.find_all(['th','td'])
        sample_cells = sample_row.find_all(['th','td'])
        days_cells   = days_row.find_all(['th','td'])

        # Платформы — это ячейки после первой
        platforms = [c.get_text(strip=True) for c in plat_cells[1:]]
        samples   = [c.get_text(strip=True) for c in sample_cells[1:]]
        days      = [c.get_text(strip=True) for c in days_cells[1:]]

        if not (len(platforms) == len(samples) == len(days)):
            raise ValueError("Число платформ не совпадает с числом значений Sample/Days")

        # 3) Построить результирующий словарь
        def maybe_int(s):
            return int(s) if s.isdigit() else s

        result = {}
        for plat, samp, dy in zip(platforms, samples, days):
            result[plat] = {
                "sample": maybe_int(samp),
                "days":   maybe_int(dy)
            }

        return result


    def get_page_info_by_title(self, space_key, page_title):
        api_url = f"{self._base_url}/rest/api/content?spaceKey={space_key}&title={page_title.replace(' ', '+')}&expand=body.storage,version"
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            'Accept': 'application/json'
        }
        response = requests.get(api_url, headers=headers)
        print(response.status_code)
        if response.status_code == 200:
            results = response.json()['results']
            if results:
                # return results[0]
                return {
                    "page_id": results[0]['id'],
                    "page_version": results[0]['version']["number"],
                    "page_title": results[0]['title'],
                    "current_content": results[0]['body']['storage']['value'],
                    "page_url": f"{self._base_url}/rest/api/content/{results[0]['id']}"
                }
            else:
                print("Page not found.")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
        return None

    def sanitize_xhtml(self, xhtml: str) -> str:
        # Remove illegal XML characters: nulls and control chars except tab, newline, carriage return
        return re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", xhtml)

    def upload_data(self, page_url, content):
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "Content-Type": "application/json"
        }
        response = requests.put(page_url, headers=headers, data=json.dumps(content))
        if response.status_code == 200:
            print("Page updated successfully!")
        else:
            print(f"Failed to update page. Status code: {response.status_code}")
            print(response.text)

    def replace_expand_section(self, url, exp_id, exp_results_content):
        # Fetch current page
        info = self.get_page_info(url)
        current = info['current_content']

        # Parse storage-format HTML/XML
        soup = BeautifulSoup(current, 'lxml')

        # Find H1 with exact text 'results'
        header = soup.find('h1', string=lambda s: s and s.strip().lower() == 'results')
        if not header:
            raise RuntimeError("H1 title 'results' not found on the page.")

        # Remove existing ui-expand macro with macro-id == exp_id immediately after header
        to_remove = None
        for sibling in header.find_next_siblings():
            if sibling.name == 'ac:structured-macro' and \
            sibling.get('ac:name') == 'ui-expand' and \
            sibling.get('ac:macro-id') == exp_id:
                to_remove = sibling
                break
        if to_remove:
            to_remove.decompose()

        # Build new expand macro
        new_macro = soup.new_tag('ac:structured-macro', **{
            'ac:name': 'ui-expand',
            'ac:macro-id': exp_id
        })
        # Title parameter (optional if you have a title)
        title_param = soup.new_tag('ac:parameter', **{'ac:name': 'title'})
        title_param.string = exp_id
        new_macro.append(title_param)

        # Insert the HTML results into the macro body
        body = soup.new_tag('ac:rich-text-body')
        fragment = BeautifulSoup(exp_results_content, 'html.parser')
        for el in fragment.contents:
            body.append(el)
        new_macro.append(body)

        # Insert right after the header
        header.insert_after(new_macro)

        # Convert back to string
        new_content = str(soup)

        # Update page with new version
        # new_version = info['page_version'] + 1
        updated_content = {
            'version': {
                'number': info['page_version'] + 1
            },
            'title': info['page_title'],
            'type': 'page',
            'body': {
                'storage': {
                    'value': self.sanitize_xhtml(new_content),
                    'representation': 'storage'
                }
            }
        }
        result = self.upload_data(info['page_url'], updated_content)
        return None

    def upload_image(self, file_path, file_name, page_id):
        with open(file_path, 'rb') as file:
            file_content = file.read()
        # encoded_content = base64.b64encode(file_content).decode('utf-8')
        upload_url = f'{self._base_url}/rest/api/content/{page_id}/child/attachment'
        # print(upload_url)
        headers = {
            'Authorization': f'Bearer {self._api_token}',
            'X-Atlassian-Token': 'no-check'
        }
        response = requests.post(upload_url, headers=headers, files={'file': (file_name, file_content, "application/octet-stream")})
        if response.status_code == 200:
            print('Image uploaded successfully')
            return response.json()['results'][0]['id']
        else:
            print(f'Failed to upload image. Status code: {response.status_code}')
            print(response.text)
        return None

    def generate_image_markup(self, image_file_name, width=250, height=250):
        image_markup = f'<ac:image ac:width="{width}" ac:height="{height}"><ri:attachment ri:filename="{image_file_name}" ri:version-at-save="1" /></ac:image>'
        return image_markup

