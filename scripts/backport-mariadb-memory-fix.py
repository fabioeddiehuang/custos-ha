#!/usr/bin/env python3
# =============================================================
# backport-mariadb-memory-fix.py — 把「MariaDB 内存封顶」回移到某个 Custos 代码树
# -------------------------------------------------------------
# 背景：修复最早做在 2.3 树（Custos-HA/）。生产环境若仍跑 2.2.20，需要同样的修复
#       落到 2.2.20 的代码树里，否则「重新生成节点」会把内存治理配置覆盖掉。
#
# 作用（全部幂等，已改过会自动跳过）：
#   1) app/node_gen.py
#      a. 插入 _MEMORY_CNF_BLOCK 常量；
#      b. 在 render_mariadb_my_cnf / render_mariadb_docker_my_cnf 的复制/性能段之后
#         注入该块（两处一起改，保证裸机与容器形态一致）；
#      c. 容器 compose 的 mariadb 服务 environment 增加 MALLOC_ARENA_MAX: "2"。
#   2) app/node_assets/Install-ha-mariadb-Node.sh
#      在 systemd 单元的 [Service] 段增加 Environment=MALLOC_ARENA_MAX=2（裸机同治理）。
#
# 用法：
#     sudo python3 backport-mariadb-memory-fix.py /path/to/Custos-HA-2.2.20-latest
#     加 --dry-run 只看将要做什么，不落盘。
# =============================================================
import ast
import sys
from pathlib import Path

MEM_BLOCK = '''# ---------------------------------------------------------------------------
# 内存封顶（根治 MariaDB 容器/裸机内存膨胀）
# ---------------------------------------------------------------------------
# 为什么要显式写死这些值（而不是依赖官方镜像/发行版默认）：
#   1) 各构建的默认值不一致，且 key_buffer_size(默认 128M) / aria_pagecache_buffer_size(默认 128M)
#      这类「固定大缓存」对以 InnoDB 为主的业务纯属浪费；显式给小值即可省下约 200M/节点。
#   2) performance_schema 若被镜像/发行版默认打开，会随运行时间**持续增长**（可达数百 MB~GB），
#      是容器内存悄悄变大的头号嫌疑；本产品不依赖它（备份/一致性巡检均显式排除该库），显式关闭。
#   3) 会话级缓冲（sort/join/read/tmp_table…）是【每连接】的，连接数一多会成倍放大——
#      必须显式封顶，避免「连接数 × 缓冲」把内存顶穿。
#   4) thread_cache_size：管理端是 2s 一轮的高频短连接，默认偏大的线程缓存会把大量线程及其
#      缓冲常驻不放，必须收紧。
# 注意：以下取值均为「不超过发行版默认」的保守上限，只封顶、不放大，故不会增加内存占用。
_MEMORY_CNF_BLOCK = (
    "# ---- 内存封顶（显式设置，避免随默认值/连接数/运行时长膨胀）----\\n"
    "# performance_schema 纯 InnoDB 业务不需要，且开启后会随运行时间持续占用内存：显式关闭。\\n"
    "performance_schema = OFF\\n"
    "# MyISAM/Aria 固定大缓存瘦身（默认各 128M，对 InnoDB 业务基本无用）\\n"
    "key_buffer_size = 16M\\n"
    "aria_pagecache_buffer_size = 32M\\n"
    "# InnoDB 其它缓冲\\n"
    "innodb_log_buffer_size = 16M\\n"
    "innodb_buffer_pool_instances = 4\\n"
    "# 表缓存 / 线程缓存 / 连接数（够用即可）\\n"
    "table_open_cache = 2000\\n"
    "table_definition_cache = 800\\n"
    "thread_cache_size = 16\\n"
    "max_connections = 200\\n"
    "# 会话级缓冲（每连接，逐个封顶，防「连接数 × 缓冲」顶穿内存）\\n"
    "sort_buffer_size = 1M\\n"
    "join_buffer_size = 256K\\n"
    "read_buffer_size = 128K\\n"
    "read_rnd_buffer_size = 256K\\n"
    "tmp_table_size = 16M\\n"
    "max_heap_table_size = 16M\\n"
    "binlog_cache_size = 32K\\n"
    "binlog_stmt_cache_size = 32K\\n"
    "net_buffer_length = 16K\\n"
)


'''

# 两个渲染函数里，复制/性能段之后紧跟的同一对片段 → 在中间注入常量引用。
_INJECT_OLD = (
    '        "slave_parallel_mode = optimistic\\n"\n'
    '        "# 慢日志（供观测/排错）\\n"'
)
_INJECT_NEW = (
    '        "slave_parallel_mode = optimistic\\n"\n'
    '        + _MEMORY_CNF_BLOCK +\n'
    '        "# 慢日志（供观测/排错）\\n"'
)

_ENV_OLD = '      MARIADB_INNODB_BUFFER_POOL_SIZE: "{buffer_pool}"\n'
_ENV_NEW = (
    '      MARIADB_INNODB_BUFFER_POOL_SIZE: "{buffer_pool}"\n'
    '      # glibc malloc 的 arena 数量上限：容器内多线程进程会为每个线程建独立 arena，\n'
    '      # 释放后常驻不归还 OS，典型表现是「RSS 只涨不降、容器内存越来越大」（可达数百 MB）。\n'
    '      # 限制为 2 可显著抑制该膨胀，是容器化 MySQL/MariaDB 内存治理的常规手段。\n'
    '      MALLOC_ARENA_MAX: "2"\n'
)

_UNIT_OLD = "LimitNOFILE=65535\n"
_UNIT_NEW = (
    "LimitNOFILE=65535\n"
    "# glibc malloc 的 arena 上限：抑制容器/裸机上「RSS 只涨不降」的经典膨胀\n"
    "# （多线程下 glibc 为每个线程建独立 arena，释放后不归还 OS，可达数百 MB）。\n"
    "Environment=MALLOC_ARENA_MAX=2\n"
)

DRY = "--dry-run" in sys.argv


def _write(p: Path, text: str):
    if DRY:
        return
    p.write_text(text, encoding="utf-8")


def _patch(path: Path, edits, label):
    """edits: [(old, new, marker_to_check_idempotency)]；任一 old 不存在则整文件不改。"""
    if not path.exists():
        print(f"  ! 跳过（文件不存在）：{path}")
        return False
    src = path.read_text(encoding="utf-8")
    out = src
    changed = False
    for old, new, marker in edits:
        if marker and marker in out:
            print(f"  = 已存在，跳过：{label} :: {marker!r}")
            continue
        if old not in out:
            print(f"  ! 锚点未命中，跳过该处：{label} :: {old.splitlines()[0][:60]!r}")
            continue
        out = out.replace(old, new)
        changed = True
        print(f"  + 已修改：{label} :: {marker or old.splitlines()[0][:40]!r}")
    if changed:
        _write(path, out)
    return changed


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print(__doc__)
        sys.exit(2)
    root = Path(args[0]).resolve()
    node_gen = root / "app" / "node_gen.py"
    installer = root / "app" / "node_assets" / "Install-ha-mariadb-Node.sh"
    print(f"目标代码树：{root}{'（dry-run，不落盘）' if DRY else ''}")

    if not node_gen.exists():
        sys.exit(f"错误：未找到 {node_gen}（确认这是 Custos 代码树根目录）")

    print("[1/2] app/node_gen.py")
    _patch(node_gen, [
        # a) 常量（插在第一个 render_mariadb_my_cnf 定义之前）
        ("def render_mariadb_my_cnf(", MEM_BLOCK + "def render_mariadb_my_cnf(", "_MEMORY_CNF_BLOCK = ("),
        # b) 两个渲染函数各注入一次
        (_INJECT_OLD, _INJECT_NEW, "+ _MEMORY_CNF_BLOCK +"),
        # c) compose 环境变量
        (_ENV_OLD, _ENV_NEW, "MALLOC_ARENA_MAX"),
    ], "node_gen.py")

    print("[2/2] app/node_assets/Install-ha-mariadb-Node.sh")
    _patch(installer, [
        (_UNIT_OLD, _UNIT_NEW, "Environment=MALLOC_ARENA_MAX=2"),
    ], "Install-ha-mariadb-Node.sh")

    # 语法自检：改坏了就明确报出来（dry-run 时跳过）
    if not DRY:
        try:
            ast.parse(node_gen.read_text(encoding="utf-8"))
            print("语法自检：node_gen.py OK")
        except SyntaxError as e:
            sys.exit(f"错误：node_gen.py 语法被破坏，请用 git 回滚后重试：{e}")
    print("完成。后续：重建管理器镜像 → 对节点「重新生成」→ 各节点重建容器。")


if __name__ == "__main__":
    main()
