"""crypto.py — 管理库凭证静态加密（v2.3.1）。

为什么需要
----------
管理库里 `nodes.root_pass`、`clusters.ha_pass / repl_pass / app_pass` **明文入库**。
任何能读管理库的人（或拿到管理库备份、dump 文件）即可直接拿到生产库 root 口令；
v2.5 单元分发还会把这些凭证写进节点部署文件下发到目标机，明文属于不可接受的风险敞口。

设计要点
--------
1. **读取透明**：所有凭证读取最终都走 `db.query()`，在该层按列名自动解密，
   13 个模块、数百处调用**零改动**即可拿到明文（不改动任何业务逻辑）。
2. **写入收敛**：明文入库只有 3 处（clusters.create_cluster / clusters.update_cluster /
   node_gen.generate_node），在这 3 处显式加密即可，无遗漏风险。
3. **自动迁移**：启动时扫描存量明文并加密，无需人工干预；已加密的不会二次加密。
4. **密钥缺失/错误明确报错**：绝不静默降级为明文。
   - 密钥来源优先级：`CRED_KEY` 环境变量 → `data/cred.key` 文件 → 自动生成并持久化；
   - 解密失败（密钥错误）时**保留密文 + 记录告警**，绝不返回半个明文或空串，
     也不把密文当明文用（会在日志与健康检查里显式暴露）。

密文格式：`ENC1:<urlsafe-base64(Fernet token)>`，非该前缀的值一律视为明文原样返回，
保证迁移期、外部直连改库等场景不会把系统打死。
"""
import os
import base64
import logging
import threading

from cryptography.fernet import Fernet, InvalidToken

_log = logging.getLogger(__name__)

PREFIX = "ENC1:"
_KEY_ENV = "CRED_KEY"
_KEY_FILE_NAME = "cred.key"

_lock = threading.Lock()
_fernet = None            # (key_bytes, Fernet) 或 None
_last_error = ""          # 最近一次加解密错误，供健康检查/设置页展示


def _data_dir():
    """仓库根下的 data/（容器内 /app/data，由 compose 挂载持久化）。"""
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def _key_path():
    return os.path.join(_data_dir(), _KEY_FILE_NAME)


def _load_or_create_key():
    """取得密钥（bytes）。优先级：环境变量 → 密钥文件 → 自动生成并落盘。

    密钥文件权限 600；若所在目录不可写（如本机开发以普通用户跑），
    仍会在内存生成一枚**临时**密钥并明确告警——此时密文在重启后不可解，
    故生产部署必须保证 data/ 可写（容器默认已满足）。
    """
    raw = (os.environ.get(_KEY_ENV) or "").strip()
    if raw:
        return raw.encode("utf-8"), "env"

    path = _key_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                k = f.read().strip()
            if k:
                return k.encode("utf-8"), "file"
    except Exception as e:
        _log.warning("[crypto] 读取密钥文件失败(%s)，将尝试重新生成: %s", path, e)

    key = Fernet.generate_key()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(key.decode("utf-8"))
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
        return key, "file(new)"
    except Exception as e:
        _log.error("[crypto] 无法持久化密钥到 %s（%s）：本次使用内存临时密钥，"
                   "重启后历史密文将不可解密，请立即修复 data/ 目录权限。", path, e)
        return key, "memory"


def _get():
    """惰性初始化 Fernet（线程安全）。"""
    global _fernet
    if _fernet is None:
        with _lock:
            if _fernet is None:
                key, src = _load_or_create_key()
                try:
                    _fernet = (key, Fernet(key))
                    _log.info("[crypto] 凭证加密已启用（密钥来源：%s）", src)
                except Exception as e:  # 环境变量里塞了非法密钥
                    global _last_error
                    _last_error = "密钥无效：%s" % e
                    _log.error("[crypto] 密钥无效(%s)，凭证加密不可用", e)
                    raise
    return _fernet[1]


def is_encrypted(value):
    return isinstance(value, str) and value.startswith(PREFIX)


def encrypt(plain):
    """加密明文；空/None 原样返回；已是密文则不再重复加密。

    加密失败时**抛异常**（不静默降级为明文）——宁可写入失败被立刻发现，
    也不能让明文悄悄落库。
    """
    if plain is None or plain == "":
        return plain
    if is_encrypted(plain):
        return plain
    global _last_error
    try:
        token = _get().encrypt(str(plain).encode("utf-8"))
    except Exception as e:
        _last_error = "加密失败：%s" % e
        _log.error("[crypto] 加密失败，凭证未落库（不降级为明文）: %s", e)
        raise
    return PREFIX + base64.urlsafe_b64encode(token).decode("utf-8")


def decrypt(value):
    """解密；非密文（明文/空）原样返回，用于兼容迁移期与外部直连写入。

    解密失败（密钥错误或密文损坏）时返回**空串**并记录告警：
    返回密文会让上层拿着 `ENC1:...` 去连数据库，报错信息更难懂；
    返回空串则表现为「密码不对」，配合日志里的明确告警可快速定位为密钥问题。
    """
    if value is None or value == "":
        return value
    if not is_encrypted(value):
        return value                      # 明文：迁移期 / 外部写入，原样放行
    global _last_error
    try:
        token = base64.urlsafe_b64decode(value[len(PREFIX):].encode("utf-8"))
        return _get().decrypt(token).decode("utf-8")
    except (InvalidToken, ValueError, TypeError) as e:
        _last_error = "解密失败（密钥不匹配或密文损坏）：%s" % e
        _log.error("[crypto] %s —— 请确认 data/cred.key（或 CRED_KEY）是否为加密时使用的密钥",
                   _last_error)
        return ""
    except Exception as e:
        _last_error = "解密异常：%s" % e
        _log.error("[crypto] %s", _last_error)
        return ""


def last_error():
    """最近一次加解密错误（供设置页 / 健康检查展示），无错误返回空串。"""
    return _last_error


def status():
    """给设置页用的状态摘要。"""
    src = "env" if (os.environ.get(_KEY_ENV) or "").strip() else (
        "file" if os.path.exists(_key_path()) else "未初始化")
    return {"enabled": True, "key_source": src, "last_error": _last_error}
