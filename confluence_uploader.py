from confluence import ConfluenceWorker



class ConfluenceUploader:
    def __init__(self) -> None:
        self.client = ConfluenceWorker()


    def publish_report(self, experiment_id: int, html_content: str, page_id: str) -> None:
        anchor = f"#{experiment_id}"
        self.client.replace_expand_section(page_id, anchor, html_content)
