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
   - 密钥来源优先级：`CRED_KEY` 环境变量（高级/外部 KMS）→ `data/cred.key` 文件（仅旧 v2.3.1 兼容）→ **从 `SECRET_KEY` 派生（默认）**；
   - 默认路径下密钥**不单独存储**：它随 `SECRET_KEY`（`.env` 固定值，或 config 自动生成并持久化到 `data/secret_key`）天然备份，
     小白「备份项目目录 / data/」即完整恢复，无需理解 key 文件、也不需镜像进库；
   - 解密失败（密钥错误）时**保留密文 + 记录告警**，绝不返回半个明文或空串，
     也不把密文当明文用（会在日志与健康检查里显式暴露）。

密文格式：`ENC1:<urlsafe-base64(Fernet token)>`，非该前缀的值一律视为明文原样返回，
保证迁移期、外部直连改库等场景不会把系统打死。
"""
import os
import base64
import hashlib
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


def _derive_from_secret(secret):
    """从 SECRET_KEY 单向派生凭证密钥（HKDF 风格的固定信息派生）。

    设计出发点（v2.3.1 收尾，替代“单独 key 文件 / 镜像进库”）：
    密钥**不单独存储**，从而既消除「小白误删 key 文件→集群失联」的运维陷阱，
    也避免把密钥写进管理库（否则库/dump 泄露即等于白加密）。

    SECRET_KEY 是部署必有之物：
      - setup.sh 安装会在 .env 固定 SECRET_KEY → 派生密钥随 .env 一同备份；
      - 未显式设置时 config 会自动生成并持久化到 data/secret_key（已挂载、随 data/ 备份）。
    因此「备份项目目录 / data/」即可完整恢复凭证，无需任何额外动作，对小白零负担。
    """
    dig = hashlib.sha256(
        b"custos-credential-key-v1\x00" + secret.encode("utf-8")
    ).digest()
    return base64.urlsafe_b64encode(dig)   # 32 字节 → 合法 Fernet 密钥


def _load_or_create_key():
    """取得密钥（bytes）。优先级：

    1. ``CRED_KEY`` 环境变量 —— 显式指定（高级用户 / 接外部 KMS、HSM）。
    2. ``data/cred.key`` 文件 —— 仅旧 v2.3.1 部署遗留兼容；**不再自动新建**。
    3. 从 ``SECRET_KEY`` 派生（默认）—— 无需任何额外文件或备份，最省心。

    第 3 项依赖 config.SECRET_KEY 一定就绪（config 保证非空且 ≥16，否则启动已中止），
    故正常路径下密钥必然可确定，不存在「生成出会变密钥」的隐患。
    """
    raw = (os.environ.get(_KEY_ENV) or "").strip()
    if raw:
        return raw.encode("utf-8"), "env(CRED_KEY)"

    path = _key_path()
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                k = f.read().strip()
            if k:
                return k.encode("utf-8"), "file(cred.key,旧部署兼容)"
    except Exception as e:
        _log.warning("[crypto] 读取密钥文件失败(%s)，改由 SECRET_KEY 派生: %s", path, e)

    from .config import SECRET_KEY as _sk
    if not _sk or len(_sk) < 16:
        raise RuntimeError(
            "SECRET_KEY 未就绪（长度不足 16），无法派生凭证密钥，启动中止。"
        )
    return _derive_from_secret(_sk), "derived(SECRET_KEY)"


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

    解密失败（密钥错误或密文损坏）时【原样返回密文】并记录告警。

    为什么绝不能返回空串：空串会被上层当成合法明文，在下一次写库时经
    ``encrypt("")`` 原样落库（encrypt 对空串/None 直接返回），从而把真实凭证
    **永久覆盖为空口令**；更糟的是 generate_node 会把空口令渲染进
    ``fix-node-creds.sql`` / ``ha_check.conf`` 下发到目标机，
    表现为「修一次坏一次、越修越坏」且原始密码不可恢复。
    返回密文则凭证不丢：故障表现为「密码不对」，配合日志与设置页的
    ``crypto.last_error()`` 可立即定位为密钥问题，换回正确 SECRET_KEY 即自愈。
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
        _log.error("[crypto] %s —— 请确认 SECRET_KEY（或 CRED_KEY）是否与加密时一致；"
                   "凭证密钥由 SECRET_KEY 派生，SECRET_KEY 变动需重新录入节点/集群凭证。"
                   "【已原样返回密文，凭证未被清空】",
                   _last_error)
        return value
    except Exception as e:
        _last_error = "解密异常：%s" % e
        _log.error("[crypto] %s【已原样返回密文，凭证未被清空】", _last_error)
        return value


def last_error():
    """最近一次加解密错误（供设置页 / 健康检查展示），无错误返回空串。"""
    return _last_error


def status():
    """给设置页用的状态摘要。"""
    src = "env" if (os.environ.get(_KEY_ENV) or "").strip() else (
        "file" if os.path.exists(_key_path()) else "derived(SECRET_KEY)")
    return {"enabled": True, "key_source": src, "last_error": _last_error,
            "mirrored_to_db": False}


def cred_status():
    """返回凭证加密状态（供「系统设置 → 凭证加密」展示）。"""
    try:
        key, src = _load_or_create_key()
        kb = (key or b"")
        masked = (kb[:4].decode("utf-8", "replace") + "…" + kb[-4:].decode("utf-8", "replace")) if kb else "—"
        return {
            "enabled": True,
            "algo": "Fernet (AES-128-CBC + HMAC-SHA256)",
            "key_source": src,
            "key_masked": masked,
            "note": "所有集群/节点凭证均以 ENC1: 前缀密文存储于管理库；密钥由上述来源派生，"
                    "备份项目 data/ 目录即可完整恢复凭证。",
        }
    except Exception as e:
        return {"enabled": False, "error": str(e)}


def bootstrap():
    """启动时确保密钥已就绪（CRED_KEY / 旧 cred.key / SECRET_KEY 派生）并返回其 base64 串。

    由 db.init_schema 在明文加密迁移前调用，保证迁移使用正确的密钥。
    密钥本身无需镜像进库、无需单独备份——它随 SECRET_KEY（.env 或 data/secret_key）
    天然备份，小白只需常规备份项目目录 / data/ 即可完整恢复。
    """
    _get()
    return _fernet[0].decode("utf-8")
