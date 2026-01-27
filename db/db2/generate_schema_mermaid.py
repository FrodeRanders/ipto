#!/usr/bin/env python3
"""
# generate_schema_mermaid.py (DB2)

Generate Mermaid ER diagrams from DB2 `schema.sql`.

## What it does
- Parses `CREATE TABLE` blocks in `schema.sql`
- Emits Mermaid `erDiagram` blocks
- Infers PK and FK columns from constraints

## Usage
Print Mermaid ERD to stdout:
```
python generate_schema_mermaid.py --schema schema.sql --markdown
```

Update the Mermaid block inside a README (default `README.md` in the current dir):
```
python generate_schema_mermaid.py --schema schema.sql --update-readme
```

Update a specific README file:
```
python generate_schema_mermaid.py --schema schema.sql --update-readme README.md
```

## Notes
- `--update-readme` replaces the first ```mermaid fenced block it finds.
- If no Mermaid block exists, it appends one at the end of the file.
- `--markdown` wraps output in a ```mermaid block; it is implied by `--update-readme`.
"""
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple


STOP_KEYWORDS = {
    "NOT",
    "NULL",
    "DEFAULT",
    "PRIMARY",
    "UNIQUE",
    "CONSTRAINT",
    "REFERENCES",
    "CHECK",
    "GENERATED",
}


@dataclass
class ForeignKey:
    from_table: str
    to_table: str
    from_cols: Tuple[str, ...]
    to_cols: Tuple[str, ...]


@dataclass
class Table:
    name: str
    columns: Dict[str, str]
    pk_cols: Tuple[str, ...]
    fk_cols: Tuple[str, ...]


def strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    lines = []
    for line in sql.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        lines.append(line)
    return "\n".join(lines)


def normalize_table_name(name: str) -> str:
    return name.split(".")[-1].strip('"')


def split_top_level_items(definition: str) -> List[str]:
    items = []
    current = []
    depth = 0
    in_string = False
    i = 0
    while i < len(definition):
        ch = definition[i]
        if ch == "'" and (i == 0 or definition[i - 1] != "\\"):
            in_string = not in_string
            current.append(ch)
        elif not in_string:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                item = "".join(current).strip()
                if item:
                    items.append(item)
                current = []
            else:
                current.append(ch)
        else:
            current.append(ch)
        i += 1
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return items


def parse_columns(items: List[str]) -> Tuple[Dict[str, str], List[str], List[ForeignKey]]:
    columns: Dict[str, str] = {}
    pk_cols: List[str] = []
    fks: List[ForeignKey] = []

    for item in items:
        upper = item.upper()
        if upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY") or upper.startswith("FOREIGN KEY"):
            pk_match = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", item, re.I)
            if pk_match:
                cols = [c.strip().strip('"') for c in pk_match.group(1).split(",")]
                pk_cols.extend(cols)
                continue
            fk_match = re.search(
                r"FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([^\s(]+)\s*\(([^)]+)\)",
                item,
                re.I,
            )
            if fk_match:
                from_cols = tuple(c.strip().strip('"') for c in fk_match.group(1).split(","))
                to_table = normalize_table_name(fk_match.group(2))
                to_cols = tuple(c.strip().strip('"') for c in fk_match.group(3).split(","))
                fks.append(ForeignKey("", to_table, from_cols, to_cols))
                continue
            continue

        tokens = item.split()
        if not tokens:
            continue
        col_name = tokens[0].strip('"')
        type_tokens = []
        for tok in tokens[1:]:
            if tok.upper() in STOP_KEYWORDS:
                break
            type_tokens.append(tok)
        if not type_tokens:
            continue
        columns[col_name] = " ".join(type_tokens).upper()

    return columns, pk_cols, fks


def parse_schema(sql: str) -> Tuple[List[Table], List[ForeignKey]]:
    tables: List[Table] = []
    all_fks: List[ForeignKey] = []

    pattern = re.compile(r"CREATE\s+TABLE\s+([^\s(]+)\s*\(", re.I)
    pos = 0
    while True:
        match = pattern.search(sql, pos)
        if not match:
            break
        table_name = normalize_table_name(match.group(1))
        start = match.end() - 1
        depth = 0
        i = start
        while i < len(sql):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        if depth != 0:
            raise ValueError(f"Unbalanced parentheses for table {table_name}")

        definition = sql[start + 1 : i]
        items = split_top_level_items(definition)
        columns, pk_cols, fks = parse_columns(items)
        table = Table(
            name=table_name,
            columns=columns,
            pk_cols=tuple(pk_cols),
            fk_cols=tuple(col for fk in fks for col in fk.from_cols),
        )
        tables.append(table)
        for fk in fks:
            all_fks.append(ForeignKey(table_name, fk.to_table, fk.from_cols, fk.to_cols))
        pos = i

    return tables, all_fks


def render_mermaid(tables: List[Table], fks: List[ForeignKey], markdown: bool) -> str:
    lines: List[str] = []
    if markdown:
        lines.append("```mermaid")
    lines.append("erDiagram")

    for table in tables:
        lines.append(f"    {table.name} {{")
        for col_name, col_type in table.columns.items():
            flags = []
            if col_name in table.pk_cols:
                flags.append("PK")
            if col_name in table.fk_cols:
                flags.append("FK")
            flag_str = f" {' '.join(flags)}" if flags else ""
            lines.append(f"        {col_type} {col_name}{flag_str}")
        lines.append("    }")
        lines.append("")

    seen = set()
    for fk in fks:
        key = (fk.from_table, fk.to_table, fk.from_cols)
        if key in seen:
            continue
        seen.add(key)
        label = ", ".join(fk.from_cols)
        lines.append(f"    {fk.to_table} ||--o{{ {fk.from_table} : \"{label}\"")

    if markdown:
        lines.append("```")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Mermaid ER diagram from schema.sql")
    parser.add_argument(
        "--schema",
        default="schema.sql",
        help="Path to schema.sql (default: schema.sql)",
    )
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Wrap output in ```mermaid fenced block",
    )
    parser.add_argument(
        "--update-readme",
        nargs="?",
        const="README.md",
        help="Replace Mermaid block in README (default: README.md)",
    )
    args = parser.parse_args()

    sql_path = Path(args.schema)
    sql = sql_path.read_text(encoding="utf-8")
    sql = strip_comments(sql)

    tables, fks = parse_schema(sql)
    markdown = args.markdown or args.update_readme is not None
    output = render_mermaid(tables, fks, markdown)

    if args.update_readme is not None:
        readme_path = Path(args.update_readme)
        content = readme_path.read_text(encoding="utf-8")
        pattern = re.compile(r"```mermaid\\s*.*?```", re.S)
        replacement = output.rstrip()
        if pattern.search(content):
            content = pattern.sub(replacement, content, count=1)
        else:
            if not content.endswith("\n"):
                content += "\n"
            content += replacement + "\n"
        readme_path.write_text(content, encoding="utf-8")
    else:
        print(output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
