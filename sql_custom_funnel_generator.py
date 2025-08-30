import re
from collections import OrderedDict, defaultdict



def clean_input(s):
    """Поддерживает как строку, так и dict {'funnel': '...'}; чистит \\xa0 и лишние пробелы."""
    if isinstance(s, dict):
        s = s.get("funnel", "")
    if s is None:
        return ""
    s = s.replace("\xa0", " ")
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return re.sub(r"\s+", " ", s).strip()

def _extract(cond: str, key: str):
    """Достаёт значение ключа key из условия (строка или число)."""
    m = re.search(rf"{key}\s*=\s*'([^']+)'", cond, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(rf"{key}\s*=\s*([0-9]+)", cond, flags=re.IGNORECASE)
    return m.group(1) if m else None

def cond_to_alias(cond: str, used: set) -> str:
    """
    Уникальный алиас:
      Event[/value][/item_id]
    При повторе полностью идентичного условия добавляется ' / 2', ' / 3', ...
    """
    event = _extract(cond, "event") or cond
    parts = [event]
    val = _extract(cond, "value")
    if val:
        parts.append(val)
    item_id = _extract(cond, "item_id")
    if item_id:
        parts.append(item_id)
    base = " / ".join(parts)
    alias = base
    i = 2
    while alias in used:
        alias = f"{base} / {i}"
        i += 1
    used.add(alias)
    return alias

def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"

def prefix_entity_fields(cond: str) -> str:
    """Добавляет префикс 'e.' к известным полям внутри условия."""
    fields = ["event", "value", "item_id", "source", "sku", "plan", "category"]
    pattern = r"(?i)(?<![A-Za-z0-9_.])(" + "|".join(fields) + r")\s*="
    return re.sub(pattern, r"e.\1=", cond)

class _DAGParser:
    """
    Парсит строку вида:
      members > event='A' and value='X' > [ event='B', event='C' > event='D' ] > event='E'
    Возвращает:
      nodes: OrderedDict[alias] = cond
      children: dict[parent_alias] = [child_alias, ...]
    """
    def __init__(self, s: str):
        self.s = s
        self.i = 0
        self.n = len(s)
        self.nodes = OrderedDict()            # alias -> cond
        self.children = defaultdict(list)     # alias -> [alias,...]
        self.used_aliases = set()

    def _skip_ws(self):
        while self.i < self.n and self.s[self.i].isspace():
            self.i += 1

    def _peek(self):
        return self.s[self.i] if self.i < self.n else ''

    def _parse_condition(self) -> str:
        start = self.i
        while self.i < self.n and self.s[self.i] not in ">;[]":
            self.i += 1
        return self.s[start:self.i].strip()

    def _ensure_node(self, cond: str) -> str:
        alias = cond_to_alias(cond, self.used_aliases)
        if alias not in self.nodes:
            self.nodes[alias] = cond
        return alias

    def _add_edge(self, src_alias: str, dst_alias: str):
        if src_alias == dst_alias:
            return
        self.children[src_alias].append(dst_alias)

    def _parse_term(self, sources):
        self._skip_ws()
        ch = self._peek()
        if ch == '[':
            self.i += 1  # consume '['
            end_union = []
            while True:
                self._skip_ws()
                if self._peek() == ']':
                    self.i += 1
                    break
                ends = self._parse_chain(list(sources))
                end_union.extend(ends)
                self._skip_ws()
                if self._peek() == ';':
                    self.i += 1
                    continue
                elif self._peek() == ']':
                    self.i += 1
                    break
                else:
                    break
            return end_union if end_union else list(sources)
        else:
            cond = self._parse_condition()
            if not cond:
                return list(sources)
            if cond.strip().lower() == 'members':
                return list(sources)
            alias = self._ensure_node(cond)
            for src in sources:
                self._add_edge(src, alias)
            return [alias]

    def _parse_chain(self, sources):
        ends = self._parse_term(sources)
        while True:
            self._skip_ws()
            if self._peek() == '>':
                self.i += 1
                ends = self._parse_term(ends)
            else:
                break
        return ends

    def parse(self):
        self._parse_chain(['members'])
        return self.nodes, self.children

def generate_clickhouse_sql(dag_def) -> str:
    dag_str = clean_input(dag_def)
    nodes, children = _DAGParser(dag_str).parse()  # nodes: alias->cond

    # DFS порядок без зацикливания
    order = []
    visited = set()
    def dfs(parent):
        for child in children.get(parent, []):
            if child not in visited:
                visited.add(child)
                order.append(child)
                dfs(child)
    dfs('members')

    # uniqExactIf-колонки
    uniq_cols = []
    for alias in order:
        cond = nodes[alias]
        uniq_cols.append(f"uniqExactIf(e.unified_id, {cond}) as {quote_ident(alias)}")
    uniq_cols_sql = ",\n        ".join(uniq_cols)

    # OR-условия
    or_lines = []
    for alias in order:
        cond_e = prefix_entity_fields(nodes[alias])
        or_lines.append(cond_e)
    or_sql = "\n        or ".join(or_lines)

    # parent_of для деноминаторов
    parent_of = {}
    for p, kids in children.items():
        for k in kids:
            # если у узла несколько родителей (из веток) — берём первого по обходу
            parent_of.setdefault(k, p)

    # базовые колонки
    select_cols = [
        "m.dt as dt",
        "m.variation as variation",
        "m.members as members",
    ]

    # счётчики и ratio вдоль рёбер
    for alias in order:
        select_cols.append(f"e.{quote_ident(alias)} as {quote_ident(alias)}")
        parent = parent_of.get(alias, 'members')
        denom = "members" if parent == "members" else quote_ident(parent)
        select_cols.append(
            f"{quote_ident(alias)} / {denom} * 100 as {quote_ident(f'{parent} -> {alias}, %')}"
        )

    # финальная конверсия members -> последний шаг (последним столбцом)
    last_alias = order[-1] if order else None
    if last_alias:
        select_cols.append(
            f"{quote_ident(last_alias)} / members * 100 as {quote_ident(f'members -> {last_alias}, %')}"
        )

    select_cols_sql = ",\n    ".join(select_cols)

    # Итоговый SQL (CTE members_agg — строго фиксированный блок из задания)
    sql = (
"""members_agg as (
    select
        toDate(m.exp_start_dt, 'UTC') as dt,
        m.variation as variation,
        uniqExact(m.unified_id) as members
    from
        members as m
    group by
        dt,
        variation
),

funnel as (
select
    {select_cols}
from
    members_agg as m
left join (
    select
        toDate(m.exp_start_dt, 'UTC') as dt,
        m.variation as variation,
        {uniq_cols}
    from
        {{table}} as e
    inner join
        members as m
    on
        e.unified_id = m.unified_id
    where
        e.date between toDate({{datetime_start}}) and '{{date}}'
    and
        e.datetime between toDateTime({{datetime_start}}) and toDateTime({{datetime_end}})
    and
        has(e.`experiments.id`, {{exp_id}})
    and
        e.unified_id > 0
    and (
        {or_conditions}
    )
    and
        dt = '{{date}}'
    group by
        dt,
        variation
) as e
on
    m.dt = e.dt
and
    m.variation = e.variation
)"""
    ).format(
        select_cols=select_cols_sql,
        uniq_cols=uniq_cols_sql,
        or_conditions=or_sql
    )
    return sql

# --- пример использования ---
if __name__ == "__main__":
    # dag = "members >\xa0event='Pre-paywall Adfree View' and value='Interstitial' > [event='Pre-paywall Adfree Close' and value='Interstitial', event='Pre-paywall Compare View' and value='Interstitial' > event='Banner Upgrade View' and value='Interstitial' > event='Banner Purchase Click' and value='Interstitial' > event='Purchase Process Finish' and value='Interstitial']"
    dag = "members > event='Tour View' > event='Tour Instrument View' > event='Banner Tour View' > event = 'Purchase Process Finish' and vale='Tour Install'"
    print(generate_clickhouse_sql(dag))
