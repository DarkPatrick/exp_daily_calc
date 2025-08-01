from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values
import requests
import re
import json
from urllib.parse import urlparse, unquote
import base64
from bs4 import BeautifulSoup




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

