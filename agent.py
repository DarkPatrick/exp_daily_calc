from openai import OpenAI
from dotenv import dotenv_values
import json
from typing import List, Iterable, Dict, Any
from pydantic import BaseModel, Field
import html
import re
from collections import OrderedDict



class ExperimentSummary(BaseModel):
    Results: List[str]
    Conclusion: str
    Next_steps: List[str] = Field(alias="Next steps")



_BRACKET_PREFIX_RE = re.compile(r'^\s*\[([^\]]+)\]\s*(.*)$')

secrets: dict = dotenv_values(".env")
OPENAI_API_KEY = secrets["gpt_api_key"]

client = OpenAI(api_key=OPENAI_API_KEY)

SCHEMA = {
    "name": "ExperimentSummary",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "Results": {
                "type": "array",
                "items": {"type": "string"}
            },
            "Conclusion": {"type": "string"},
            "Next steps": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["Results", "Conclusion", "Next steps"]
    },
    "strict": True
}



def generate_gpt_prompt(clients_options: dict, config_dict: dict, exp_results: list, exp_solution: str):
    prompt = f"""
Ты — аналитик экспериментов. Разбери CSV-таблицы по правилам ниже и верни сжатый вывод.

Правила:

    Столбцы — метрики. В каждой таблице первая строка — контроль.

    Для каждой тестовой ветки идут три строки: metric values, % diff (к контролю), pvalue.

    Считаем значимыми эффекты при pvalue < 0.05 (если не указано иное в Solution).

    В Results перечисли только статистически значимые изменения (±), сгруппировав по типу метрик (из description) и platform/segment и по ветке. Формат пунктов:
    "[platform / segment / variation] metric: +X%, кратко смысл (рост/падение)".

    Если значимых изменений нет в группе — ничего для неё не пиши.

    Если значимые изменения по группам повторяются, то суммируй ответ в общих чертах. например:в двух сегментах упал ARPU на 15% и 17%, то пиши ARPU decreased by 15%-17%
    Если значимые изменения повторяются в схожих метриках, то суммируй ответ в общих чертах. например, большая часть Tab View метрик просела: пиши Tab View metrics decreased by x%-y%
    Если сильно значимо просело много метрик, то достаточно указать самые важные: access cr, % и arpu
    Только не схлопывай группы платформ: по WEB / ANDROID / IOS всегда должны быть отделные резульататы

    В Conclusion дай управленческую рекомендацию (раскатывать/не раскатывать/доисследовать), учитывая баланс метрик.

    В Next steps предложи 1-3 конкретных шагов с приоритетами.
    весь текст пиши по-ангийски

Входные данные:

{exp_solution}
"""
    for client in clients_options:
        for segment in config_dict:
            prompt += f"""
            во всех табилцах одинаковый формат:
            1 строка - контрольная вариация Control
            2 строка - тестовая вариация Variation #2
            3 строка - diff, % = (Variation #2 - Control) / Control * 100
            4 строка - pvalue
            для каждой тестовой вариации (если есть, то Variation #3, Variation #4, ...) добавляются по 3 аналогичные строки
                тестовая вариация Variation #X
                diff, % = (Variation #X - Control) / Control * 100
                pvalue
            ### table
            description=monetization metics
            platform={client}
            segment={segment}
            ```csv
            {exp_results[client][segment]['monetization']['cum_metrics'].to_csv()}
            ### table
            description=retention metics
            platform={client}
            segment={segment}
            ```csv
            {exp_results[client][segment]['retention']['cum_metrics'].to_csv()}
            ### table
            description=tab view metics
            platform={client}
            segment={segment}
            ```csv
            {exp_results[client][segment]['long_tab_view']['cum_metrics'].to_csv()}
            """

    return prompt


def ask_gpt_opinion(prompt: str):
    # response = client.responses.create(
    response = client.responses.parse(
        model="gpt-5",
        input=[{
            "role": "user",
            # "content": [{"type": "input_text", "text": prompt}]
            "content": prompt
        }],
        text_format=ExperimentSummary
        # text_format={
        #     "type": "json_schema",
        #     "json_schema": SCHEMA
        # }
    )
    # print(response.output_parsed)
    # print(response.output)
    obj: ExperimentSummary = response.output_parsed
    # return json.loads(response.output_text)
    return obj.model_dump(by_alias=True, exclude_none=True)


def _esc(t: Any) -> str:
    return html.escape(str(t)).replace("\n", "<br/>")


def _li_paragraph(text_html: str) -> str:
    # <li><p>...</p></li> — так Confluence корректно рендерит
    return f"  <li><p>{text_html}</p></li>"


def _ul(lis: List[str]) -> str:
    return "<ul>\n" + "\n".join(lis) + "\n</ul>" if lis else "<p>—</p>"


def _group_results(items: Iterable[str]) -> OrderedDict[str, List[str]]:
    groups: "OrderedDict[str, List[str]]" = OrderedDict()
    singles: List[str] = []
    for raw in items or []:
        s = str(raw).strip()
        if not s:
            continue
        m = _BRACKET_PREFIX_RE.match(s)
        if not m:
            singles.append(s)
            continue
        key, tail = m.groups()
        tail = tail.lstrip(" \t-—")  # убираем лидирующее тире/пробел
        groups.setdefault(key, []).append(tail)
    # отдельная «группа» для строк без префикса — ключ = "" (в конец)
    if singles:
        groups[""] = singles
    return groups


def results_to_storage_html(results: Iterable[str]) -> str:
    groups = _group_results(results)
    lis: List[str] = []

    for key, tails in groups.items():
        if key == "":
            # без префикса — обычные одноуровневые пункты (как есть)
            for t in tails:
                lis.append(_li_paragraph(_esc(t)))
            continue

        key_html = f"<code>&#91;{_esc(key)}&#93;</code>"

        if len(tails) == 1:
            # одиночный пункт — в одну строку: [prefix] tail
            text = f"{key_html} {_esc(tails[0])}" if tails[0] else key_html
            lis.append(_li_paragraph(text))
        else:
            # несколько пунктов — делаем подсписок
            sublis = [_li_paragraph(_esc(t)) for t in tails]
            inner_ul = _ul(sublis)
            lis.append(
                # важно: вложенный UL находится ВНУТРИ <li> и сразу после <p>
                "  <li>\n"
                f"    <p>{key_html}</p>\n"
                f"    {inner_ul}\n"
                "  </li>"
            )

    return _ul(lis)


def gpt_advice_to_confluence_html(summary: Dict[str, Any]) -> str:
    results = summary.get("Results") or summary.get("results") or []
    conclusion = summary.get("Conclusion") or summary.get("conclusion") or ""
    next_steps = (
        summary.get("Next steps")
        or summary.get("Next_steps")
        or summary.get("next_steps")
        or []
    )

    parts = [
        "\n\n",
        "<h2>Decision</h2>",
        "<p><strong>Results</strong></p>",
        results_to_storage_html(results),
        "<p><strong>Conclusion</strong></p>",
        # f"<p>{html.escape(str(conclusion)).replace('\n', '<br/>') or '—'}</p>",
        # f"<p>{html.escape(str(conclusion)) or '—'}</p>",
        f"<p>{_esc(conclusion) if str(conclusion).strip() else '—'}</p>",
        "<p><strong>Next steps</strong></p>",
        # _bullets_simple(next_steps),
        _ul([_li_paragraph(_esc(x)) for x in (next_steps or [])]) if next_steps else "<p>—</p>",
        "\n\n"
    ]
    return "\n".join(parts)