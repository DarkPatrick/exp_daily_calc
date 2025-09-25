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
            first_cell = rows[0].find(['th', 'td'])
            print("first_cell=",first_cell,"target_header=", target_header)
            # it can be a button inside with text that i am looking for
            if not first_cell or first_cell.get_text(strip=True) != target_header:
                button = first_cell.find(['button'])
                if not button or button.get_text(strip=True) != target_header:
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
        target_header = f"#{id_value} audience"

        # 1) Найти таблицу по заголовку в первой строке
        table = None
        for tbl in soup.find_all('table'):
            # print(tbl)
            tbody = tbl.find('tbody')
            thead = tbl.find('thead')
            if not tbody:
                continue
            rows_body = tbody.find_all('tr')
            rows_head = None
            if thead:
                rows_head = thead.find_all('tr')
            if not rows_body:
                continue
            # первая строка, первый столбец может быть <th> или <td>
            first_cell_body = rows_body[0].find(['th', 'td'])
            # print("CELL=", first_cell_body.get_text(strip=True))
            
            first_cell_head = None
            if rows_head:
                first_cell_head = rows_head[0].find(['th', 'td'])
                # print("CELL HEAD=", first_cell_head.get_text(strip=True))
            
            if first_cell_body and first_cell_body.get_text(strip=True).lower() == target_header or first_cell_head and first_cell_head.get_text(strip=True).lower() == target_header:
                table = tbl
                break

        # print("HERE_0")
        if table is None:
            # raise ValueError(f"Таблица с заголовком '{target_header}' не найдена")
            return {}

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

        # print("HERE_1")
        if not platform_row or not sample_row or not days_row:
            print("Не найдены строки Platform, Sample или Days в таблице")
            # raise ValueError("Не найдены строки Platform, Sample или Days в таблице")
            return {}

        # Извлечь списки ячеек (в т.ч. могут быть <td> или <th>)
        plat_cells   = platform_row.find_all(['th','td'])
        sample_cells = sample_row.find_all(['th','td'])
        days_cells   = days_row.find_all(['th','td'])

        # Платформы — это ячейки после первой
        platforms = [c.get_text(strip=True) for c in plat_cells[1:]]
        samples   = [c.get_text(strip=True) for c in sample_cells[1:]]
        days      = [c.get_text(strip=True) for c in days_cells[1:]]
        
        # print(platforms, samples, days)

        # print("HERE_2")
        if not (len(platforms) == len(samples) == len(days)):
            # raise ValueError("Число платформ не совпадает с числом значений Sample/Days")
            return {}

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


    def _norm_text(self, node) -> str:
        return node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""


    def _find_h1_section(self, soup: BeautifulSoup, title: str):
        for h in soup.find_all(['h1']):
            if self._norm_text(h) == title:
                return h
        return None


    def _iter_section_nodes(self, h1):
        """
        Идем по элементам после данного h1 до следующего h1 (не включая его).
        """
        for el in h1.next_elements:
            if getattr(el, "name", None) == "h1":
                break
            # интересуют только теги
            if getattr(el, "name", None):
                yield el


    def _next_nonempty_in_section(self, start_node, section_nodes):
        """
        Возвращает следующий по порядку в секции элемент с непустым текстом.
        """
        found = False
        for el in section_nodes:
            if el is start_node:
                found = True
                continue
            if found:
                txt = self._norm_text(el)
                if txt:
                    return el
        return None


    def extract_solution_bullets(self, html: str, exp_id: str) -> str:
        def _is_solution_node(node) -> bool:
            return self._norm_text(node).lower() == "solution"

        def _find_solution_with_exp_id(section_nodes, exp_marker: str):
            for node in section_nodes:
                if _is_solution_node(node):
                    nxt = self._next_nonempty_in_section(node, section_nodes)
                    if nxt and self._norm_text(nxt) == exp_marker:
                        return node
            return None

        def _find_first_solution(section_nodes):
            for node in section_nodes:
                if _is_solution_node(node):
                    return node
            return None

        def _find_nearest_list_after(anchor, section_nodes):
            """
            Ищем ближайший <ul>/<ol> после anchor в пределах той же секции.
            Сначала — среди его следующих элементов (next_elements),
            ограничиваясь узлами секции, затем — среди его потомков на всякий случай.
            """
            seen_anchor = False
            for el in section_nodes:
                if el is anchor:
                    seen_anchor = True
                    continue
                if seen_anchor and getattr(el, "name", None) in ("ul", "ol"):
                    return el

            # fallback: если список вложен глубже в самом anchor
            for el in anchor.descendants:
                if getattr(el, "name", None) in ("ul", "ol"):
                    return el
            return None

        def _li_text_without_sublists(li) -> str:
            parts = []
            for child in li.contents:
                name = getattr(child, "name", None)
                if name in ("ul", "ol"):
                    continue
                if hasattr(child, "get_text"):
                    text = child.get_text(" ", strip=True)
                else:
                    text = str(child).strip()
                if text:
                    parts.append(text)
            return " ".join(parts).strip()

        def _render_list_recursive(tag, level, out_lines):
            if tag.name not in ("ul", "ol"):
                return
            # прямые <li> без захода вглубь (чтобы корректно обработать вложенность)
            for li in tag.find_all("li", recursive=False):
                text = _li_text_without_sublists(li)
                out_lines.append(("  " * level) + "- " + text if text else ("  " * level) + "-")
                # обрабатываем вложенные списки, если есть
                for sub in li.find_all(["ul", "ol"], recursive=False):
                    _render_list_recursive(sub, level + 1, out_lines)

        def _render_list(list_tag) -> str:
            """Преобразует <ul>/<ol> в строку с маркерами `- ` и отступами для вложенностей."""
            lines = []
            _render_list_recursive(list_tag, 0, lines)
            return "\n".join(lines)

        soup = BeautifulSoup(html, 'html.parser')
        h1 = self._find_h1_section(soup, "Description of the Solution & Mockups")
        if not h1:
            return ''

        section_nodes = list(self._iter_section_nodes(h1))

        # 1) Пытаемся найти 'Solution' + следующий непустой элемент '#{exp_id}'
        target_pair = f"#{exp_id}".strip()
        solution_node = _find_solution_with_exp_id(section_nodes, target_pair)

        # 2) Если связка не найдена — берём первый 'Solution'
        if solution_node is None:
            solution_node = _find_first_solution(section_nodes)
            if solution_node is None:
                return ''

        # 3) Ищем ближайший список после найденного 'Solution'
        list_tag = _find_nearest_list_after(solution_node, section_nodes)
        if list_tag is None:
            return ''

        # 4) Рендерим список в текст c буллетами и отступами
        return _render_list(list_tag)


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
        return re.sub(r"<!--.*?-->", "", re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", xhtml), flags=re.S)

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

