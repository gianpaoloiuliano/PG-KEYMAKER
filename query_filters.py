import re
from dataclasses import dataclass, field

import subpattern_extractor_v2 as sub_ext


# ---------------------------------------------------------------------------
# QUERY FILTERS
# ---------------------------------------------------------------------------
# Filters are a free-form Cypher string, e.g.:
#   "p.age < 18 AND en1.cost IS NOT NULL AND avg(en1.cost) > 100 PER p"
#
# - the string is split on the top-level ANDs (outside parentheses
#   and strings): each piece is a filter
# - SIMPLE filter: Cypher condition on the pattern aliases, goes into the WHERE
# - AGGREGATE filter: contains avg/min/max/sum/count/... and MUST end with
#   "PER <node_alias>" (grouping alias). The aggregate is computed
#   for each <node_alias> over the minimal contiguous portion of the pattern that
#   contains <node_alias> and the aliases used inside the aggregate (scope).
#   It thus becomes a filter on the <node_alias> node only.
#
# A filter applies to a tree node (leaf, triple, subpattern) if
# all its aliases appear in that node.

# Level of the [FILTERS] prints:
#   0 → no prints
#   1 → summary: filters read, how many tree nodes they apply to, occurrences
#   2 → detail: additionally generated queries, rows per node, APOC outcome, leaf cache
DEBUG_FILTRI = 1

_AGG_FUNCS = r'avg|min|max|sum|count|collect|stDevP|stDev|percentileCont|percentileDisc'
_AGG_RE = re.compile(rf'\b({_AGG_FUNCS})\s*\(', re.IGNORECASE)
_PER_RE = re.compile(r'^(.*\S)\s+PER\s+([A-Za-z_]\w*)\s*$', re.IGNORECASE | re.DOTALL)
_STRING_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
# identifiers not preceded by '.' (so 'p.age' yields 'p' and not 'age')
_IDENT_RE = re.compile(r'(?<![\w.$])([A-Za-z_]\w*)\b')


def dbg(msg, level=1):
    if DEBUG_FILTRI >= level:
        print(f"[FILTERS] {msg}")


@dataclass
class Filter:
    text: str                                   # original text of the filter
    aliases: set = field(default_factory=set)   # aliases needed to apply it
    is_aggregate: bool = False
    group_alias: str = None                     # aggregates only
    agg_calls: list = field(default_factory=list)  # [(var_name, "avg(en1.cost)"), ...]
    cond: str = None                            # condition on the aggregate vars
    scope_pattern: str = None                   # pattern on which to compute the aggregate
    ok_var: str = None                          # list of valid group_aliases
    idx: int = None                             # filter number (#k in the prints)

    def __repr__(self):
        if self.is_aggregate:
            return f"Filter(AGG per {self.group_alias}: {self.text!r})"
        return f"Filter({self.text!r})"


# ---------------------------------------------------------------------------
# PARSING
# ---------------------------------------------------------------------------

def _mask_strings(s: str) -> str:
    """Replaces the content of string literals with spaces (same length)."""
    return _STRING_RE.sub(lambda m: ' ' * len(m.group(0)), s)


def split_top_level_and(s: str) -> list[str]:
    """Splits s on the ANDs outside parentheses and string literals."""
    masked = _mask_strings(s)
    parts = []
    depth = 0
    start = 0
    i = 0
    n = len(masked)
    while i < n:
        c = masked[i]
        if c in '([{':
            depth += 1
        elif c in ')]}':
            depth -= 1
        elif depth == 0 and masked[i:i + 3].upper() == 'AND':
            before = masked[i - 1] if i > 0 else ' '
            after = masked[i + 3] if i + 3 < n else ' '
            if not (before.isalnum() or before == '_') and not (after.isalnum() or after == '_'):
                parts.append(s[start:i])
                start = i + 3
                i += 3
                continue
        i += 1
    parts.append(s[start:])
    return [p.strip() for p in parts if p.strip()]


def _find_aliases(text: str, known_aliases) -> set:
    masked = _mask_strings(text)
    return {m.group(1) for m in _IDENT_RE.finditer(masked) if m.group(1) in known_aliases}


# namespaces of Cypher functions written as "x.y(...)" that are not aliases
_CYPHER_NAMESPACES = {'apoc', 'date', 'datetime', 'localdatetime', 'localtime', 'time',
                      'duration', 'point', 'math', 'string', 'db', 'gds'}
_PROP_ACCESS_RE = re.compile(r'(?<![\w.$])([A-Za-z_]\w*)\s*\.')


def _check_unknown_aliases(text: str, known_aliases):
    masked = _mask_strings(text)
    unknown = {m.group(1) for m in _PROP_ACCESS_RE.finditer(masked)
               if m.group(1) not in known_aliases and m.group(1).lower() not in _CYPHER_NAMESPACES}
    if unknown:
        raise ValueError(
            f"In filter {text!r} the aliases {sorted(unknown)} do not appear in the pattern "
            f"(available aliases: {sorted(known_aliases)})"
        )


def _extract_agg_calls(text: str, prefix: str):
    """
    Finds the aggregate calls (with balanced parentheses) and replaces them with
    variables. Returns (text_with_variables, [(var_name, call), ...]).
    """
    masked = _mask_strings(text)
    calls = []
    out = []
    last = 0
    for m in _AGG_RE.finditer(masked):
        if m.start() < last:
            continue  # aggregate nested inside another: stays in the outer call
        depth = 0
        j = m.end() - 1  # position of the '('
        while j < len(masked):
            if masked[j] == '(':
                depth += 1
            elif masked[j] == ')':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        if depth != 0:
            raise ValueError(f"Unbalanced parentheses in filter: {text!r}")
        var = f"{prefix}_{len(calls)}"
        calls.append((var, text[m.start():j + 1]))
        out.append(text[last:m.start()])
        out.append(var)
        last = j + 1
    out.append(text[last:])
    return ''.join(out), calls


def _scope_pattern(tokens, alias_index, aliases) -> str:
    """Minimal contiguous portion of the pattern that contains all the given aliases."""
    idx = [alias_index[a] for a in aliases]
    lo, hi = min(idx), max(idx)
    # the portion must start and end with a node (even indices)
    if lo % 2 == 1:
        lo -= 1
    if hi % 2 == 1:
        hi += 1
    return sub_ext.tokens_to_pattern(tokens[lo:hi + 1])


def parse_filters(filter_str, pattern) -> list[Filter]:
    """Turns the filter string into a list of Filters, validated against the pattern."""
    if not filter_str or not filter_str.strip():
        return []

    tokens = sub_ext.tokenize_pattern(pattern)
    alias_index = sub_ext.tokens_alias_index(tokens)
    node_aliases = {a for a, i in alias_index.items() if i % 2 == 0}

    filters = []
    for k, part in enumerate(split_top_level_and(filter_str)):
        _check_unknown_aliases(part, alias_index)
        if _AGG_RE.search(_mask_strings(part)):
            m = _PER_RE.match(part)
            if not m:
                raise ValueError(
                    f"Aggregate filter without 'PER <alias>': {part!r} "
                    f"(e.g. 'avg(en.cost) > 100 PER p')"
                )
            body, group = m.group(1), m.group(2)
            if group not in node_aliases:
                raise ValueError(
                    f"Grouping alias '{group}' is not a node of the pattern "
                    f"(available nodes: {sorted(node_aliases)})"
                )
            cond, calls = _extract_agg_calls(body, f"__agg{k}")
            inner_aliases = set()
            for _, call in calls:
                inner_aliases |= _find_aliases(call, alias_index)
            # aliases outside the aggregates: only the group is allowed
            outer_aliases = _find_aliases(cond, alias_index)
            if outer_aliases - {group}:
                raise ValueError(
                    f"In aggregate filter {part!r} the aliases {sorted(outer_aliases - {group})} "
                    f"are used outside the aggregate: only the group alias '{group}' is allowed"
                )
            f = Filter(
                text=part,
                aliases={group},
                is_aggregate=True,
                group_alias=group,
                agg_calls=calls,
                cond=cond.strip(),
                scope_pattern=_scope_pattern(tokens, alias_index, inner_aliases | {group}),
                ok_var=f"__ok{k}",
            )
            dbg(f"#{k} aggregate  {part}   (per {group}, computed on {f.scope_pattern})")
        else:
            aliases = _find_aliases(part, alias_index)
            f = Filter(text=part, aliases=aliases)
            dbg(f"#{k} simple      {part}   (aliases {', '.join(sorted(aliases))})")
        f.idx = k
        filters.append(f)
    return filters


# ---------------------------------------------------------------------------
# APPLICABILITY / CACHE
# ---------------------------------------------------------------------------


def applicable_filters(filters, aliases) -> list[Filter]:
    """Filters whose aliases are all contained in `aliases` (used by the GFD baseline)."""
    return [f for f in (filters or []) if f.aliases <= set(aliases)]


def filters_for_subpattern(filters, sub_str) -> list[Filter]:
    """
    Filters applicable to a sub-pattern given as a string (used by the HyUCC
    and HPIValid baselines): single edge "[r:R]", single node "(a:A)" or path.
    """
    if not filters:
        return []
    sub_str = sub_str.strip()
    if sub_str.startswith('['):
        aliases = {sub_ext._parse_edge_token(sub_str)[0]}
    else:
        aliases = set(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(sub_str)))
    return applicable_filters(filters, aliases)


def return_star(pattern_str, filters) -> str:
    """
    Replacement for "RETURN *" for the HyUCC and HPIValid baselines: with aggregate filters
    "*" would also return the __okK lists of the subqueries, which would end up as
    columns in the DataFrame. In that case it returns the pattern aliases, otherwise "*".
    """
    if not any(f.is_aggregate for f in (filters or [])):
        return "*"
    return ", ".join(sub_ext.tokens_alias_index(sub_ext.tokenize_pattern(pattern_str.strip())))


def filters_label(filters) -> str:
    """
    Suffix for the prints of the pattern under analysis, e.g. "  [filters #0 p.gender = 'F' | #2 count(en1) > 100 PER p]".
    Empty string if the node has no filters or if DEBUG_FILTRI = 0.
    """
    if not filters or DEBUG_FILTRI < 1:
        return ""
    return "  [filters " + " | ".join(f"#{f.idx} {f.text}" for f in filters) + "]"


def filter_signature(filters, alias) -> str:
    """
    Signature of a leaf's filters with the alias replaced by a placeholder.
    Two leaves with the same label can share keys only if
    they have the same signature. No filters → "".
    """
    if not filters:
        return ""
    alias_re = re.compile(rf'(?<![\w.$]){re.escape(alias)}\b')
    return " AND ".join(sorted(alias_re.sub('§', f.text) for f in filters))


def baseline_subpatterns(pattern) -> list[str]:
    """
    Sub-patterns used by the HyUCC and HPIValid baselines, obtained with the same
    tokenizer as PG-KEYMAKER (supports -[r:R]-, -[r:R]->, <-[r:R]-):
      - single nodes "(a:A)" and single edges "[r:R]"
      - all contiguous node→node paths with at least one edge, whole pattern
        included, with the original arrows preserved
    """
    tokens = sub_ext.tokenize_pattern(pattern.strip())
    subs = [t for t in tokens[0::2]]
    subs += [sub_ext.connector_raw_token(t) for t in tokens[1::2]]
    for start in range(0, len(tokens), 2):
        for end in range(start + 2, len(tokens), 2):
            subs.append(sub_ext.tokens_to_pattern(tokens[start:end + 1]))
    return list(dict.fromkeys(subs))


# ---------------------------------------------------------------------------
# QUERY BUILDING
# ---------------------------------------------------------------------------

def build_match_block(pattern, filters, extra_where=None) -> str:
    """
    Builds the MATCH ... WHERE ... part of a query.
    Without filters and without extra_where it returns exactly "MATCH <pattern>".

    Each aggregate filter becomes an isolated subquery executed only once,
    which collects the valid values of the group alias:
        CALL () { MATCH <scope> WITH g, avg(...) AS __aggK_0 WHERE <cond> RETURN collect(g) AS __okK }
    and the main WHERE contains "g IN __okK".
    """
    filters = filters or []
    lines = []
    where = []

    for f in filters:
        if f.is_aggregate:
            g = f.group_alias
            with_items = ", ".join([g] + [f"{call} AS {var}" for var, call in f.agg_calls])
            lines.append(
                f"CALL () {{ MATCH {f.scope_pattern} WITH {with_items} "
                f"WHERE {f.cond} RETURN collect({g}) AS {f.ok_var} }}"
            )
            where.append(f"{g} IN {f.ok_var}")
        else:
            where.append(f"({f.text})")

    if extra_where:
        where.append(extra_where)

    lines.append(f"MATCH {pattern}")
    if where:
        lines.append("WHERE " + " AND ".join(where))

    block = "\n".join(lines)
    if filters:
        dbg(f"query for {pattern}:\n{block}", level=2)
    return block


# ---------------------------------------------------------------------------
# QUICK SELF-TEST (without database)
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import neo4j_querying_module as n4j

    pattern = "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)-[nx:NEXT]-(en2:Encounter)-[hasp:HAS_PROCEDURE]-(pro:Procedure)"
    filtri = ("p.gender = 'F' AND (en1.cost > 10 OR en1.cost IS NULL) AND nx.days < 30 "
              "AND avg(en1.cost) > 100 PER p AND count(pro) >= 2 PER en2 AND pro.description <> 'AND'")

    fs = parse_filters(filtri, pattern)
    print()
    G = sub_ext.build_pattern_graph(pattern, fs)
    print()
    for node in ["(p:Patient)", "[nx:NEXT]", "(en2:Encounter)",
                 "(p:Patient)-[has:HAS_ENCOUNTER]-(en1:Encounter)", pattern]:
        print("=" * 70)
        print(n4j.generate_query_from_pattern(node, G.nodes[node]["filters"]))
    print("=" * 70)
    print("signature en1:", filter_signature(G.nodes["(en1:Encounter)"]["filters"], "en1"))
    print("signature en2:", filter_signature(G.nodes["(en2:Encounter)"]["filters"], "en2"))

    for bad in ["avg(en1.cost) > 100", "zz.age > 3 AND max(en1.cost) > 1 PER has"]:
        try:
            parse_filters(bad, pattern)
        except ValueError as e:
            print("expected error:", e)
