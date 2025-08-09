import re
from collections import OrderedDict, defaultdict



def clean_input(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\xa0", " ")
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1]
    return re.sub(r"\s+", " ", s).strip()

def cond_to_alias(cond: str) -> str:
    m = re.search(r"event\s*=\s*'([^']+)'", cond, flags=re.IGNORECASE)
    return m.group(1) if m else cond

def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"

class _DAGParser:
    def __init__(self, s: str):
        self.s = s
        self.i = 0
        self.n = len(s)
        self.nodes = OrderedDict()           # cond -> alias
        self.children = defaultdict(list)    # parent_alias -> [child_alias,...]

    def _skip_ws(self):
        while self.i < self.n and self.s[self.i].isspace():
            self.i += 1

    def _peek(self):
        return self.s[self.i] if self.i < self.n else ''

    def _parse_condition(self) -> str:
        start = self.i
        while self.i < self.n and self.s[self.i] not in ">,[]":
            self.i += 1
        return self.s[start:self.i].strip()

    def _add_node(self, cond: str, alias: str):
        if cond not in self.nodes:
            self.nodes[cond] = alias

    def _add_edge(self, src_alias: str, dst_alias: str):
        self.children[src_alias].append(dst_alias)

    def _parse_term(self, sources):
        self._skip_ws()
        ch = self._peek()
        if ch == '[':
            self.i += 1
            end_union = []
            while True:
                self._skip_ws()
                if self._peek() == ']':
                    self.i += 1
                    break
                ends = self._parse_chain(list(sources))
                end_union.extend(ends)
                self._skip_ws()
                if self._peek() == ',':
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
            alias = cond_to_alias(cond)
            self._add_node(cond, alias)
            for src in sources:
                if src != alias:
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

def generate_clickhouse_sql(dag_str: str) -> str:
    dag_str = clean_input(dag_str)
    nodes, children = _DAGParser(dag_str).parse()

    # порядок (лево->право) обходом от members
    order = []
    def dfs(parent):
        for child in children.get(parent, []):
            if child not in order:
                order.append(child)
            dfs(child)
    dfs('members')

    alias_to_cond = {alias: cond for cond, alias in nodes.items()}

    uniq_cols = []
    for alias in order:
        cond = alias_to_cond[alias]
        uniq_cols.append(f"uniqExactIf(e.unified_id, {cond}) as {quote_ident(alias)}")
    uniq_cols_sql = ",\n        ".join(uniq_cols)

    or_lines = []
    for alias in order:
        cond = alias_to_cond[alias]
        parts = [p.strip() for p in cond.split('and') if p.strip()]
        or_lines.append(" and ".join([f"e.{p}" for p in parts]))
    or_sql = "\n        or ".join(or_lines)

    select_cols = [
        "m.dt as dt",
        "m.variation as variation",
        "m.members as members",
    ]

    # кто чей родитель
    parent_of = {}
    for p, kids in children.items():
        for k in kids:
            parent_of[k] = p

    # последний шаг DAG (если есть)
    last_alias = order[-1] if order else None

    # счётчики и ratio по рёбрам; ratio для последнего шага, если его родитель == members, отложим,
    # чтобы поставить ЭТОТ ratio последним столбцом
    deferred_last_ratio = None

    for alias in order:
        select_cols.append(f"e.{quote_ident(alias)} as {quote_ident(alias)}")
        parent = parent_of.get(alias, 'members')
        denom = "members" if parent == "members" else quote_ident(parent)
        ratio_sql = f"{quote_ident(alias)} / {denom} * 100 as {quote_ident(f'{parent} -> {alias}, %')}"
        if last_alias and alias == last_alias and parent == 'members':
            deferred_last_ratio = ratio_sql  # добавим в самом конце
        else:
            select_cols.append(ratio_sql)

    # Всегда добавляем финальную конверсию members -> last_alias последней колонкой
    if last_alias:
        final_ratio_sql = f"{quote_ident(last_alias)} / members * 100 as {quote_ident(f'members -> {last_alias}, %')}"
        # если это тот же шаг и мы его отложили — используем отложенный (он эквивалентен)
        if deferred_last_ratio is not None:
            # на случай, если хочется дословно 'members -> last' в алиасе, а не '{parent} -> {last}'
            select_cols.append(final_ratio_sql)
        else:
            # parent != members — добавляем дополнительную итоговую конверсию
            select_cols.append(final_ratio_sql)

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
