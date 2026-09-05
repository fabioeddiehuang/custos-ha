#!/usr/bin/env bash
# Custos 敏感文件/目录权限加固
#
# 背景：节点 root 密码以明文存于管理库（nodes.root_pass）与 generated_nodes/ 部署文件，
#       决策为**不做可逆加密**（加密只是防君子且徒增故障面），改为收紧文件权限 +
#       管理库账号最小权限。本脚本负责文件这一半。
#
# 用法：
#   ./scripts/harden_perms.sh            # dry-run：只报告差异，不改任何权限（默认）
#   ./scripts/harden_perms.sh --apply    # 真正执行 chmod
#
# ⚠ 两个实测踩过的坑，动手前务必看：
#
# 1) .env **不要**设成 600！它是 root:root 时，运行 docker compose 的普通用户
#    会直接报 `open .env: permission denied`，deploy.sh / compose 全部失效
#    （2026-09-05 实测）。正确做法是 640 + 属组给到运维所在的组：
#      sudo chown root:docker .env && sudo chmod 640 .env
#    这样 other 仍无权限（比默认的 664 更安全），同时 compose 可用。
#
# 2) 若 custos-server 容器以**非 root** 用户运行，把目录改成 700 会导致容器内
#    进程无权读写（表现为启动失败/无法写入 data）。先确认：
#      docker exec custos-server id
#    默认 root 则安全。

set -uo pipefail

APPLY=0
for a in "$@"; do [ "$a" = "--apply" ] && APPLY=1; done

# 目标:权限（顺序即报告顺序）
TARGETS=(
  ".env:640"
  "data:700"
  "data/secret_key:600"
  "generated_nodes:700"
  "backups:700"
)

perm_of() { stat -c '%a' "$1" 2>/dev/null || echo "-"; }
human() { stat -c '%U:%G' "$1" 2>/dev/null || echo "-"; }

echo "== Custos 敏感文件权限检查 =="
[ "$APPLY" = "1" ] && echo "模式：APPLY（将修改权限）" || echo "模式：DRY-RUN（只报告，不修改）"
echo

changed=0
for t in "${TARGETS[@]}"; do
  path="${t%:*}"; want="${t##*:}"
  if [ ! -e "$path" ]; then
    printf '  %-22s %s\n' "$path" "(不存在，跳过)"
    continue
  fi
  cur=$(perm_of "$path")
  if [ "$cur" = "$want" ]; then
    printf '  [OK]   %-22s %-4s (期望 %s) %s\n' "$path" "$cur" "$want" "$(human "$path")"
  else
    printf '  [需改] %-22s %-4s -> %-4s %s\n' "$path" "$cur" "$want" "$(human "$path")"
    if [ "$APPLY" = "1" ]; then
      if chmod "$want" "$path" 2>/dev/null; then
        echo "         已修改 -> $(perm_of "$path")"
        changed=$((changed + 1))
      else
        echo "         ✗ 修改失败（权限不足？）"
      fi
    fi
  fi
done

echo
echo "== gitignore 覆盖检查（防止敏感内容误入库）=="
if [ -f .gitignore ]; then
  for d in ".env" "data/" "generated_nodes/" "backups/"; do
    if grep -qxF "$d" .gitignore || grep -qxF "${d%/}" .gitignore; then
      printf '  [OK]   %-20s 已忽略\n' "$d"
    else
      printf '  [警告] %-20s 未在 .gitignore 中！\n' "$d"
    fi
  done
else
  echo "  未找到 .gitignore"
fi

echo
echo "== 提醒 =="
echo "  · 管理库账号请遵循最小权限：只授予本库（如 GRANT ALL ON ha2.* TO 'ha2'@'<网段>'），"
echo "    不要 GRANT ALL ON *.*；这也是「账号收敛」的一部分。"
echo "  · backups/*.sql.gz 内含**全量业务数据**，泄露后果比密码更严重，务必与 .env 同等保护。"
echo
if [ "$APPLY" = "1" ]; then
  echo "完成：已修改 $changed 项。"
else
  echo "DRY-RUN 结束：未修改任何权限。确认无误后加 --apply 执行。"
fi
