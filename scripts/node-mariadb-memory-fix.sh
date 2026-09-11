#!/usr/bin/env bash
# =============================================================
# node-mariadb-memory-fix.sh — Custos 节点 MariaDB 内存封顶「就地修补」
# -------------------------------------------------------------
# 用途：在【已运行】的节点上直接落地内存治理配置并重建生效。
#       与管理器/代码版本无关，**2.2.20 与 2.3 生产节点都可直接用**。
#
# 为什么需要它（生产曾出现 MariaDB 容器内存持续变大）：
#   ① performance_schema 常开：会随运行时长持续吃内存（数百 MB~GB）；
#   ② key_buffer_size / aria_pagecache_buffer_size 默认各 128M，对纯 InnoDB 业务纯浪费；
#   ③ 会话级缓冲（sort/join/read/tmp_table…）是【每连接】的，连接一多成倍放大；
#   ④ thread_cache_size 偏大 + 管理端 2s 一轮高频短连接 → 线程及其缓冲常驻不放；
#   ⑤ glibc malloc arena 每线程一个、释放不归还 OS → 典型「RSS 只涨不降」。
#   本脚本把 ①②③④ 写进 my.cnf，⑤ 通过容器环境变量 / systemd 环境变量解决。
#
# 用法（容器化 MariaDB 节点，在该节点部署目录内执行，需 root）：
#     cd <节点部署目录>          # 目录内应有 docker-compose.yaml 与 docker-my.cnf
#     sudo bash node-mariadb-memory-fix.sh
#
# 用法（裸机 MariaDB 节点，由 systemd 托管）：
#     sudo bash node-mariadb-memory-fix.sh --bare
#
# 幂等：可重复执行（已打标即跳过）。
# 回滚：原文件会先备份为 <原文件>.bak.<时间戳>，恢复后重建/重启即可。
# =============================================================
set -euo pipefail

MARK="内存封顶"
FORCE_BARE=0
[[ "${1:-}" == "--bare" ]] && FORCE_BARE=1

log(){  echo -e "\033[1;32m[mem-fix]\033[0m $*"; }
warn(){ echo -e "\033[1;33m[warn]\033[0m $*"; }
die(){  echo -e "\033[1;31m[error]\033[0m $*" >&2; exit 1; }

[[ "$(id -u)" -eq 0 ]] || die "请用 root 执行（sudo bash $0）"

# ---------- 要写入的配置块 ----------
read -r -d '' MEM_BLOCK <<'EOF' || true
# ---- 内存封顶（显式设置，避免随默认值/连接数/运行时长膨胀）----
# performance_schema 纯 InnoDB 业务不需要，且开启后会随运行时间持续占用内存：显式关闭。
performance_schema = OFF
# MyISAM/Aria 固定大缓存瘦身（默认各 128M，对 InnoDB 业务基本无用）
key_buffer_size = 16M
aria_pagecache_buffer_size = 32M
# InnoDB 其它缓冲
innodb_log_buffer_size = 16M
innodb_buffer_pool_instances = 4
# 表缓存 / 线程缓存 / 连接数（够用即可）
table_open_cache = 2000
table_definition_cache = 800
thread_cache_size = 16
max_connections = 200
# 会话级缓冲（每连接，逐个封顶，防「连接数 × 缓冲」顶穿内存）
sort_buffer_size = 1M
join_buffer_size = 256K
read_buffer_size = 128K
read_rnd_buffer_size = 256K
tmp_table_size = 16M
max_heap_table_size = 16M
binlog_cache_size = 32K
binlog_stmt_cache_size = 32K
net_buffer_length = 16K
EOF

# 把内存块插入 [mysqld] 段末尾（下一个 [section] 之前）；找不到 [mysqld] 则报错不做。
patch_cnf(){
  local f="$1"
  [[ -f "$f" ]] || die "未找到配置文件：$f"
  if grep -q "$MARK" "$f"; then
    log "已含内存封顶块，跳过：$f"
    return 0
  fi
  local bak="$f.bak.$(date +%Y%m%d%H%M%S)"
  cp -a "$f" "$bak"
  if ! awk -v blk="$MEM_BLOCK" '
        BEGIN{ inmy=0; done=0 }
        /^\[mysqld\]/{ inmy=1 }
        /^\[/ && inmy && !done && $0 !~ /^\[mysqld\]/{ print blk; done=1 }
        { print }
        END{ if (inmy && !done) print blk; else if (!inmy) exit 3 }
      ' "$f" > "$f.new"; then
    rm -f "$f.new"
    die "在 $f 中未找到 [mysqld] 段，未做修改（原文件已备份：$bak）"
  fi
  mv "$f.new" "$f"
  log "已写入内存封顶块：$f（原文件备份：$bak）"
}

summary(){
  echo
  log "完成。验证方式："
  echo "  • 生效参数：  mysql -uroot -p -e \"SHOW VARIABLES LIKE 'performance_schema'\" （应为 OFF）"
  echo "  • 真实内存：  docker exec custos-mariadb sh -c \"ps -eo rss,comm | grep -i maria\""
  echo "  • 注意：docker stats 的内存含【可回收 page cache】，不等于真实占用；"
  echo "          看内存请以 memory.stat 的 anon 为准（file 部分是缓存）。"
}

# =============================================================
# 容器化 MariaDB（compose）
# =============================================================
if [[ "$FORCE_BARE" -eq 0 ]] && { [[ -f docker-compose.yaml ]] || [[ -f docker-compose.yml ]]; }; then
  COMPOSE_FILE="docker-compose.yaml"; [[ -f docker-compose.yaml ]] || COMPOSE_FILE="docker-compose.yml"
  CNF="docker-my.cnf"
  [[ -f "$CNF" ]] || CNF="my.cnf"
  log "模式：容器化 MariaDB（$COMPOSE_FILE / $CNF）"

  patch_cnf "$CNF"

  # glibc arena 上限：用 compose override 叠加，不改动生成文件本身。
  OV="docker-compose.override.yaml"
  if [[ -f "$OV" ]] && grep -q "MALLOC_ARENA_MAX" "$OV"; then
    log "已含 MALLOC_ARENA_MAX，跳过：$OV"
  else
    [[ -f "$OV" ]] && cp -a "$OV" "$OV.bak.$(date +%Y%m%d%H%M%S)"
    cat > "$OV" <<'EOF'
# Custos 内存治理（Docker Compose 自动合并本文件，故不修改生成的 docker-compose.yaml）
# glibc malloc arena 上限：抑制「RSS 只涨不降」（多线程下每线程一个 arena、释放不归还 OS）。
services:
  mariadb:
    environment:
      MALLOC_ARENA_MAX: "2"
EOF
    log "已写入 $OV（MALLOC_ARENA_MAX=2）"
  fi

  log "重建 mariadb 容器使静态参数生效（--force-recreate）…"
  docker compose -f "$COMPOSE_FILE" up -d --force-recreate mariadb
  sleep 3
  docker compose -f "$COMPOSE_FILE" ps mariadb || true
  summary
  exit 0
fi

# =============================================================
# 裸机 MariaDB（systemd 单元 mariadb-ha.service）
# =============================================================
log "模式：裸机 MariaDB（systemd）"
CNF="/etc/my.cnf"
[[ -f "$CNF" ]] || die "未找到 $CNF"
patch_cnf "$CNF"

DROPIN_DIR="/etc/systemd/system/mariadb-ha.service.d"
DROPIN="$DROPIN_DIR/zz-memory.conf"
if [[ -f "$DROPIN" ]] && grep -q "MALLOC_ARENA_MAX" "$DROPIN"; then
  log "已含 MALLOC_ARENA_MAX，跳过：$DROPIN"
else
  mkdir -p "$DROPIN_DIR"
  cat > "$DROPIN" <<'EOF'
# Custos 内存治理：glibc malloc arena 上限（抑制「RSS 只涨不降」）
[Service]
Environment=MALLOC_ARENA_MAX=2
EOF
  log "已写入 systemd drop-in：$DROPIN"
fi

systemctl daemon-reload
log "重启 mariadb-ha.service 使配置生效…"
systemctl restart mariadb-ha.service
sleep 3
systemctl --no-pager status mariadb-ha.service | head -12 || true
summary
