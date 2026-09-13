# custosctl —— bash 补全定义
#
# 安装后通常位于 /usr/share/bash-completion/completions/custosctl，bash-completion
# 会按命令名自动加载。手动立即生效：
#   source /usr/share/bash-completion/completions/custosctl
#   （或 source 本文件）
#
# zsh 用户：在 ~/.zshrc 加
#   autoload -U bashcompinit && bashcompinit
#   source /usr/share/bash-completion/completions/custosctl

_custosctl_completions() {
  local cur prev
  COMPREPLY=()
  cur="${COMP_WORDS[COMP_CWORD]}"
  prev="${COMP_WORDS[COMP_CWORD-1]}"

  local top_cmds="deploy up rebuild start stop restart down ps tail logs shell status version rebuild-node install uninstall node help"
  local node_cmds="install start stop restart restart-keepalived status backup restore uninstall"

  # 上一个词是目录型选项：补全目录
  case "$prev" in
    -C|--dir|--target)
      COMPREPLY=( $(compgen -d -- "$cur") )
      return 0
      ;;
    # 取值型选项无需补全
    --host|--port|--cluster|--url|--user|--pass)
      return 0
      ;;
  esac

  # 是否已进入 node 上下文（custosctl node ...）
  local in_node=0 node_idx=0 i
  for (( i = 1; i < COMP_CWORD; i++ )); do
    if [ "${COMP_WORDS[i]}" = "node" ]; then
      in_node=1; node_idx=$((i + 1)); break
    fi
  done

  if [ "$in_node" = 1 ]; then
    case "$cur" in
      -*) COMPREPLY=( $(compgen -W "--dir" -- "$cur") ) ;;
      *)
        if [ "$COMP_CWORD" = "$node_idx" ]; then
          COMPREPLY=( $(compgen -W "$node_cmds" -- "$cur") )
        else
          case "${COMP_WORDS[node_idx]}" in
            backup)   COMPREPLY=( $(compgen -W "--target" -- "$cur") ) ;;
            restore)  COMPREPLY=( $(compgen -f -- "$cur") ) ;;
            uninstall) COMPREPLY=( $(compgen -W "-f" -- "$cur") ) ;;
            *)        COMPREPLY=() ;;
          esac
        fi
        ;;
    esac
    return 0
  fi

  # 紧接 node 之后：补全节点子命令
  if [ "$prev" = "node" ]; then
    COMPREPLY=( $(compgen -W "$node_cmds" -- "$cur") )
    return 0
  fi

  # 顶层子命令的选项补全
  case "$prev" in
    tail|logs)     COMPREPLY=( $(compgen -W "-n --no-follow" -- "$cur") ) ;;
    deploy|up)     COMPREPLY=( $(compgen -W "-d --build -b" -- "$cur") ) ;;
    down)          COMPREPLY=( $(compgen -W "--volumes" -- "$cur") ) ;;
    rebuild-node)  COMPREPLY=( $(compgen -W "--host --port --cluster --url --user --pass" -- "$cur") ) ;;
    uninstall)     COMPREPLY=( $(compgen -W "-f --purge --yes" -- "$cur") ) ;;
  esac

  # 默认：顶层子命令 / 全局选项（-C/--dir）；其余原样透传给 docker compose
  if [ -z "${COMPREPLY[*]:-}" ]; then
    case "$cur" in
      -*) COMPREPLY=( $(compgen -W "-C --dir" -- "$cur") ) ;;
      *)  COMPREPLY=( $(compgen -W "$top_cmds" -- "$cur") ) ;;
    esac
  fi
}

complete -F _custosctl_completions custosctl
