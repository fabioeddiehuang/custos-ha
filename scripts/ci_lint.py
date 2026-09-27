#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ci_lint.py — Custos 稳定性「静态门禁」（v2.5 改造，对应 MR5）。

解决「修了又复现」的**制度性**根因：没有门禁，同类 bug 一定换场景再犯。
本脚本在 CI / pre-commit / 构建前运行，发现即失败，覆盖 5 类历史复发坑：

  1. 裸 `except:`（吞异常，错误不可见）            —— MR2
  2. `except ...: pass` 单行静默吞（无日志）       —— MR2
  3. 在「非授权文件」直拼风味敏感 SQL：
       GTID_SUBSET(  /  super_read_only  /  IDENTIFIED WITH  /  IDENTIFIED VIA
     —— 只允许出现在 app/mysql_compat.py 与 app/ha_client/db_ops.py  —— MR1
  4. ha_client 子模块使用 detect_flavour/F_MARIADB/detect_syntax/F_MYSQL8
     但未导入（拆包漏导入曾导致运行时 NameError）                 —— MR1
  5. 前端模板 <div> 开闭不平衡（页面变形头号元凶，UI-001 两次复发） —— 易用/MR3

用法：
    python3 scripts/ci_lint.py [--app-dir app] [--templates-dir app/templates]
退出码 0=通过，1=发现违规（打印明细）。
"""
import argparse
import ast
import os
import re
import sys

# 风味敏感 SQL 只允许出现的「授权文件」
# - mysql_compat.py：兼容层（含风味分支说明）
# - db_ops.py：风味操作唯一出口（本改造新增）
# - node_gen.py：节点侧 SQL 模板生成器（单一生成点，产出在节点执行）
_ALLOWED_FLAVOR_FILES = {
    "app/mysql_compat.py",
    "app/ha_client/db_ops.py",
    "app/node_gen.py",
}
# 节点侧 shell 脚本在真实节点跑，不在此 Python 门禁范围
# 注意：只拦「真实调用」——
#   GTID_SUBSET 后紧跟 % 或 '（如 GTID_SUBSET(%s / GTID_SUBSET('%s'），放行文档里的
#     "绝不能用 GTID_SUBSET()" 这类说明文字；
#   super_read_only 只拦 SET GLOBAL super_read_only（设值），放行 @@super_read_only（读值）。
_FLAVOR_PATTERNS = [
    (re.compile(r"GTID_SUBSET\(\s*['%]"), "GTID_SUBSET("),
    (re.compile(r"SET GLOBAL super_read_only"), "SET GLOBAL super_read_only"),
    (re.compile(r"IDENTIFIED\s+WITH\b"), "IDENTIFIED WITH"),
    (re.compile(r"IDENTIFIED\s+VIA\b"), "IDENTIFIED VIA"),
]
# ha_client 子模块必须导入的风味符号
_REQUIRED_FLAVOR_IMPORTS = {
    "detect_flavour": "from app.mysql_compat import detect_flavour",
    "F_MARIADB": "from app.mysql_compat import F_MARIADB",
    "detect_syntax": "from app.mysql_compat import detect_syntax",
    "F_MYSQL8": "from app.mysql_compat import F_MYSQL8",
}


def _py_files(app_dir):
    for root, _dirs, files in os.walk(app_dir):
        for f in files:
            if f.endswith(".py"):
                yield os.path.join(root, f)


def check_bare_except(path, lines):
    """裸 except:（硬性错误）与 except ...: pass 单行静默吞（警告，逐步硬化）。

    返回 (行号, 信息, 级别)，级别 "error" 阻断 CI，"warn" 仅警告（MR2）。
    """
    issues = []
    for i, ln in enumerate(lines):
        s = ln.strip()
        # 裸 except: —— 最危险，必须拦截
        if s == "except:":
            issues.append((i + 1, "裸 except: 禁止（至少捕获 Exception）", "error"))
            continue
        # except Exception as e: \n    pass  （单行静默吞，建议记录日志）
        m = re.match(r"except\s+.*:\s*$", s)
        if m:
            nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
            if nxt == "pass":
                issues.append((i + 1, "except 后直接 pass 静默吞异常，建议记录日志或显式处理（MR2）", "warn"))
    return issues


def check_flavor_sql(rel, lines):
    """非授权文件直拼风味敏感 SQL。"""
    if rel in _ALLOWED_FLAVOR_FILES:
        return []
    issues = []
    for i, ln in enumerate(lines):
        s = ln.strip()
        # 跳过注释行与文档字符串边界行（避免把说明文字误判为直拼 SQL）
        if s.startswith("#") or s.startswith('"""') or s.startswith("'''") \
                or '"""' in ln or "'''" in ln:
            continue
        for pat, name in _FLAVOR_PATTERNS:
            if pat.search(ln):
                issues.append((i + 1, "风味敏感 SQL '%s' 必须收敛到 db_ops/mysql_compat（禁止直拼）" % name))
                break
    return issues


def check_ha_client_imports(path, rel, src):
    """ha_client 子模块用风味符号却未导入。"""
    if not rel.startswith("app/ha_client/") or rel == "app/ha_client/__init__.py":
        return []
    if rel in ("app/ha_client/db_ops.py", "app/ha_client/_const.py"):
        return []
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return [(0, "AST 解析失败: %s" % e)]
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for n in node.names:
                imported.add(n.asname or n.name)
        elif isinstance(node, ast.Import):
            for n in node.names:
                imported.add((n.asname or n.name).split(".")[0])
    used = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used.add(node.id)
    issues = []
    for sym, hint in _REQUIRED_FLAVOR_IMPORTS.items():
        if sym in used and sym not in imported and hint.split()[-1] not in imported:
            issues.append((0, "用到 %s 但未导入（%s）" % (sym, hint)))
    return issues


def check_div_balance(rel, src):
    """模板 <div> 开闭不平衡（来自待修问题清单的 html.parser 自检）。"""
    if not rel.endswith(".html"):
        return []
    VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input',
            'link', 'meta', 'param', 'source', 'track', 'wbr'}
    try:
        from html.parser import HTMLParser

        class P(HTMLParser):
            def __init__(s):
                super().__init__(convert_charrefs=True)
                s.stack = []
                s.err = []

            def handle_starttag(s, t, a):
                if t in VOID:
                    return
                s.stack.append(t)

            def handle_endtag(s, t):
                if t in VOID:
                    return
                for j in range(len(s.stack) - 1, -1, -1):
                    if s.stack[j] == t:
                        del s.stack[j:]
                        return
                s.err.append(t)

        p = P()
        p.feed(src)
    except Exception as e:
        return [(0, "div 自检异常: %s" % e)]
    if p.stack or p.err:
        return [(0, "模板 <div> 不平衡：未闭合=%s 多余闭合=%s" % (p.stack, p.err))]
    return []


def main():
    ap = argparse.ArgumentParser()
    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(here)
    ap.add_argument("--app-dir", default=os.path.join(root, "app"))
    ap.add_argument("--templates-dir", default=os.path.join(root, "app", "templates"))
    args = ap.parse_args()

    all_errors = []
    all_warns = []
    for path in _py_files(args.app_dir):
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        with open(path, encoding="utf-8") as f:
            src = f.read()
        lines = src.splitlines()
        for ln, msg, lvl in check_bare_except(path, lines):
            (all_errors if lvl == "error" else all_warns).append((rel, ln, "[静默异常] " + msg))
        for ln, msg in check_flavor_sql(rel, lines):
            all_errors.append((rel, ln, "[风味SQL] " + msg))
        for ln, msg in check_ha_client_imports(path, rel, src):
            all_errors.append((rel, ln, "[缺导入] " + msg))

    if os.path.isdir(args.templates_dir):
        for root2, _d, files in os.walk(args.templates_dir):
            for fn in files:
                if fn.endswith(".html"):
                    p = os.path.join(root2, fn)
                    rel = os.path.relpath(p, root).replace(os.sep, "/")
                    with open(p, encoding="utf-8") as f:
                        src = f.read()
                    for ln, msg in check_div_balance(rel, src):
                        all_errors.append((rel, ln, "[模板] " + msg))

    if all_warns:
        print("=" * 60)
        print("ci_lint 警告（不阻断，建议逐步硬化）%d 处：" % len(all_warns))
        for rel, ln, msg in all_warns:
            print("  %s:%s  %s" % (rel, ln, msg))
        print("=" * 60)
    if all_errors:
        print("=" * 60)
        print("ci_lint 错误（阻断）%d 处：" % len(all_errors))
        for rel, ln, msg in all_errors:
            print("  %s:%s  %s" % (rel, ln, msg))
        print("=" * 60)
        sys.exit(1)
    print("ci_lint 通过：无裸 except / 直拼风味 SQL / ha_client 缺导入 / <div> 不平衡")
    sys.exit(0)


if __name__ == "__main__":
    main()
