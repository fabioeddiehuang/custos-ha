#!/bin/bash
# keepalived 启动前预处理：
# 各节点网卡名不同（如 ens192 / ens18 / ens34），无法在管理端用单一 interface 配置覆盖，
# 故始终根据本节点 IP（unicast_src_ip）自动探测其所在网卡并写入 interface，确保 VIP 在
# 正确的网卡上漂移。
# keepalived.conf 以 :ro 只读挂载，不能就地 sed -i（Device or resource busy），先 cp 到
# 可写目录 /run 再改，最后以 -f 指向副本运行，不改动宿主机配置文件。
set -e
SRC=/etc/keepalived/keepalived.conf
RUN=/run/keepalived.conf
cp "$SRC" "$RUN"
CFG=$RUN

# 校验 VIP 是否为合法 IPv4：防止上游生成了空 VIP 配置（virtual_ipaddress 只剩 /24），
# 否则 keepalived 会静默报 “No VIP specified”，VIP 永远不上浮且极难排查。
# 命中即明确报错退出，便于在 docker logs 中立刻发现。
VIP_RAW=$(awk '/^[[:space:]]*virtual_ipaddress[[:space:]]*\{/{f=1;next} /^[[:space:]]*\}/{f=0} f && $1 !~ /^#/ && $1 ~ /^[0-9]/ {print $1; exit}' "$CFG")
VIP_ADDR=${VIP_RAW%%/*}
if ! echo "$VIP_ADDR" | grep -Eq '^[0-9]{1,3}(\.[0-9]{1,3}){3}$'; then
    echo "[entrypoint] 致命错误：keepalived.conf 的 virtual_ipaddress 中未找到合法 VIP（当前值: '${VIP_RAW}'）。"
    echo "[entrypoint] 请确认集群已设置正确的 VIP，并重新生成节点部署文件后再部署。"
    exit 1
fi

LOCAL_IP=$(grep -oP 'unicast_src_ip[[:space:]]+\K[0-9.]+' "$CFG" || true)
IFACE=""
if [ -n "$LOCAL_IP" ]; then
    # 精确匹配本节点 IP 所在网卡（排除 lo）；ip 输出地址带 /前缀，需拆分比较
    IFACE=$(ip -o -4 addr show | awk -v ip="$LOCAL_IP" '
        $2!="lo" { split($4, a, "/"); if (a[1]==ip) { print $2; exit } }')
fi
# 兜底：取首个非 lo 的 up 网卡
if [ -z "$IFACE" ]; then
    IFACE=$(ip -o -4 addr show up | awk '$2!="lo" {print $2; exit}')
fi

if [ -n "$IFACE" ]; then
    sed -E "s/^([[:space:]]*)interface[[:space:]].*/\1interface $IFACE/" "$CFG" > "$CFG.new"
    mv "$CFG.new" "$CFG"
    echo "[entrypoint] 使用网卡 $IFACE 承载 VIP（节点 IP=$LOCAL_IP）"
else
    echo "[entrypoint] 警告：未能探测到可用网卡，沿用配置值"
fi
exec keepalived --dont-fork --log-console -f "$RUN"
