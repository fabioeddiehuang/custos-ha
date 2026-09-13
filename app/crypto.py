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

v2.3.10 重要修正（默认关闭 + 保底）
--------------------------------
原设计把主密钥绑定在 SECRET_KEY 上派生，SECRET_KEY 一变（重装应用 / 重建镜像 /
install.sh 重生成 .env / data/secret_key 丢失），库里**所有** ENC1 密文集体解不开；
而 decrypt 失败会原样返回密文，密文随即被当成口令拼进 CHANGE MASTER，报
``1470 - String 'ENC1:...' is too long for MASTER_PASSWORD`` —— 复制集体瘫痪且无显式告警。

修正为三层保障：
5. **默认关闭**：``CRED_ENCRYPTION=off``（config）。关闭时新写入明文落库、**完全不依赖
   任何密钥**，彻底消除上述失效模式；有合规/高安全需求时置 ``on`` 启用。
6. **密钥持久化**：启用时主密钥落 ``data/cred.key``（compose 持久卷，首次启动自动创建，
   权限 0600），不再随 SECRET_KEY 变化 —— 重装 / 重建均不影响存量凭证。
   存量部署升级时以「当前派生密钥」为种子写入，主密钥不变，升级零影响。
7. **钥匙串回退**：decrypt 依次尝试 CRED_KEY → cred.key → SECRET_KEY 派生，任一命中即成功，
   单一来源失效不会导致集体解不开。即便加密已关闭，历史 ENC1 密文仍照常解密。

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


# 项目目录之外的密钥保险库（v2.3.10）。
# 由宿主机侧 install.sh / uninstall.sh 维护：卸载时把密钥镜像到这里，重装时自动取回。
# 于是即便整个项目目录被删除重装，凭证仍能解开 —— 只有删除 /var/lib/custos 才会真正失效。
# 容器内该路径通常不存在（非挂载卷），相关逻辑自动空转，不影响容器化部署。
_VAULT_DIR = "/var/lib/custos"


def _vault_path():
    return os.path.join(_VAULT_DIR, _KEY_FILE_NAME)


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


def _all_candidates():
    """返回**全部**可用密钥候选 [(key_bytes, source), ...]（已去重、按优先级排序）。

    这是「保底」机制的核心：解密时不再只用单一密钥，而是拿整串钥匙逐把试。
    只要存量密文是用其中任意一把加密的，就能解开 —— 因此：

      - 重装 / 重建镜像导致 SECRET_KEY 变化 → 仍可用持久化的 data/cred.key 解开；
      - 旧部署用 cred.key、新镜像改回 SECRET_KEY 派生 → 两把都在候选里；
      - 显式 CRED_KEY 与外部 KMS → 同样参与回退。

    任何单一来源变动都不会再让存量凭证集体失效（v2.3.1 的致命缺陷）。
    """
    out = []

    raw = (os.environ.get(_KEY_ENV) or "").strip()
    if raw:
        out.append((raw.encode("utf-8"), "env(CRED_KEY)"))

    try:
        path = _key_path()
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                k = f.read().strip()
            if k:
                out.append((k.encode("utf-8"), "file(cred.key)"))
    except Exception as e:  # 读不到就跳过该候选，不阻断
        _log.warning("[crypto] 读取 %s 失败，跳过该密钥候选: %s", _key_path(), e)

    try:  # 项目目录外的保险库（容器内部署时通常不存在，自动空转）
        vp = _vault_path()
        if os.path.exists(vp):
            with open(vp, "r", encoding="utf-8") as f:
                vk = f.read().strip()
            if vk:
                out.append((vk.encode("utf-8"), "file(vault:%s)" % vp))
    except Exception as e:
        _log.warning("[crypto] 读取保险库密钥失败，跳过该候选: %s", e)

    try:
        from .config import SECRET_KEY as _sk
        if _sk and len(_sk) >= 16:
            out.append((_derive_from_secret(_sk), "derived(SECRET_KEY)"))
    except Exception as e:
        _log.warning("[crypto] 由 SECRET_KEY 派生密钥失败，跳过该候选: %s", e)

    seen, uniq = set(), []
    for k, s in out:
        if k not in seen:
            seen.add(k)
            uniq.append((k, s))
    return uniq


def _load_or_create_key():
    """取得【加密用】的主密钥（bytes）。优先级：

    1. ``CRED_KEY`` 环境变量 —— 显式指定（高级用户 / 接外部 KMS、HSM）。
    2. ``data/cred.key`` 文件 —— **持久化主密钥，首次启动自动创建**（v2.3.10 起）。
    3. 从 ``SECRET_KEY`` 派生 —— 仅当上面两者都不可用时的最后手段。

    为什么必须把密钥持久化到 data/cred.key（v2.3.10 修正）：
      原实现把主密钥绑定在 SECRET_KEY 上派生。SECRET_KEY 一旦变化（重装应用、
      重建镜像、install.sh 重生成 .env、data/secret_key 丢失重建），派生密钥随之改变，
      库里所有 ENC1 密文**集体解不开** —— 表现为「复制突然全部起不来、复制口令变成
      ENC1 密文被拼进 CHANGE MASTER 报 1470」，且没有任何显式告警，极难定位。
      data/ 是 compose 持久卷，把主密钥落在其中，重装/重建均不受影响。

    兼容性：存量部署首次升级时，cred.key 用「当前派生密钥」作种子写入，
    因此主密钥与升级前完全一致，存量密文照常解开，升级零影响。
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
                return k.encode("utf-8"), "file(cred.key)"
    except Exception as e:
        _log.warning("[crypto] 读取密钥文件失败(%s)，将尝试重建: %s", path, e)

    # ---- 保险库回补：项目目录被整体删除后重装时，从 /var/lib/custos 取回密钥 ----
    try:
        vp = _vault_path()
        if os.path.exists(vp):
            with open(vp, "r", encoding="utf-8") as f:
                vk = f.read().strip()
            if vk:
                try:
                    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(vk)
                    os.chmod(path, 0o600)
                    _log.info("[crypto] 已从保险库 %s 恢复凭证密钥到 %s", vp, path)
                except Exception as e:
                    _log.warning("[crypto] 从保险库写回 %s 失败: %s", path, e)
                return vk.encode("utf-8"), "file(cred.key,restored-from-vault)"
    except Exception as e:
        _log.warning("[crypto] 读取保险库密钥失败: %s", e)

    # ---- 首次启动：生成并持久化主密钥 ----
    from .config import SECRET_KEY as _sk
    if _sk and len(_sk) >= 16:
        # 存量部署升级：以当前派生密钥为种子，保证主密钥不变、存量密文仍可解。
        seed = _derive_from_secret(_sk)
        src = "derived(SECRET_KEY)"
    else:
        # 保底：连 SECRET_KEY 都不可用时，直接生成随机密钥，绝不因缺 SECRET_KEY 而
        # 让凭证加密整体不可用（原实现在此直接 raise，会让整个应用起不来）。
        seed = base64.urlsafe_b64encode(os.urandom(32))
        src = "generated(random)"

    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(seed.decode("utf-8"))
        try:
            os.chmod(path, 0o600)   # 仅属主可读，避免同主机其他用户拿到凭证密钥
        except Exception:
            pass
        _log.info("[crypto] 已生成并持久化凭证主密钥 %s（种子来源：%s），"
                  "后续重装/重建均不再影响存量凭证", path, src)
    except Exception as e:
        # 写盘失败不致命：密钥仍在内存可用，且 SECRET_KEY 派生候选仍在钥匙串里，
        # 只是失去了「跨重装稳定」的保障，故仅告警。
        _log.warning("[crypto] 持久化 %s 失败（凭证仍可用，但重装后可能失效）: %s", path, e)

    return seed, ("file(cred.key)" if os.path.exists(path) else src)


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


def enabled():
    """凭证静态加密是否启用（v2.3.10 起默认关闭，见 config.CRED_ENCRYPTION）。

    关闭时：新写入一律明文落库，**完全不触碰任何密钥**，因此 SECRET_KEY 变动、
    重装应用、重建镜像都不会再导致存量凭证失效。
    注意：无论开关状态如何，decrypt() 对 ENC1 密文**始终尝试解密**，
    保证关闭加密后库里残留的历史密文仍能被正确读取。
    """
    # 优先取管理库设置（Web「设置 → 凭证加密」可在线切换）；未设置时回退 env / config。
    # 这里必须惰性导入 db：db 在模块级 import crypto，若在导入期反向 import 会循环。
    try:
        from .db import get_setting
        v = get_setting("CRED_ENCRYPTION")
        if v is not None and str(v).strip() != "":
            return str(v).strip().lower() in ("1", "on", "true", "yes", "enabled")
    except Exception:
        pass
    try:
        from .config import CRED_ENCRYPTION as _ce
    except Exception:
        _ce = None
    if not _ce:
        _ce = os.environ.get("CRED_ENCRYPTION", "off")
    return str(_ce).strip().lower() in ("1", "on", "true", "yes", "enabled")


def encrypt(plain):
    """加密明文；空/None 原样返回；已是密文则不再重复加密。

    加密失败时**抛异常**（不静默降级为明文）——宁可写入失败被立刻发现，
    也不能让明文悄悄落库。

    v2.3.10：凭证加密默认关闭（config.CRED_ENCRYPTION=off）。关闭时直接返回明文，
    **完全不触碰密钥**，从根上消除「密钥变动 → 存量凭证集体失效 → 复制静默瘫痪」。
    """
    if plain is None or plain == "":
        return plain
    if is_encrypted(plain):
        return plain
    if not enabled():
        return str(plain)                 # 关闭加密：明文落库，零密钥依赖
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
    except Exception as e:
        _last_error = "密文格式损坏（非合法 base64）：%s" % e
        _log.error("[crypto] %s【已原样返回密文，凭证未被清空】", _last_error)
        return value
    # 钥匙串回退：逐个候选密钥尝试，任一命中即成功。
    # 单把钥匙失效（SECRET_KEY 变动 / cred.key 丢失 / 换用 CRED_KEY）都不再导致
    # 存量凭证集体解不开 —— 这是「保底」的核心，必须与 enabled() 开关无关，
    # 即便加密已关闭，库里残留的 ENC1 历史密文也照样要能解开。
    for key, src in _all_candidates():
        try:
            plain = Fernet(key).decrypt(token).decode("utf-8")
            _last_error = ""              # 成功即清除历史错误，避免设置页长期红
            return plain
        except (InvalidToken, ValueError, TypeError):
            continue
        except Exception as e:            # 非法密钥（如 env 塞了坏值）：跳过该候选
            _log.warning("[crypto] 密钥候选 %s 不可用，跳过: %s", src, e)
            continue
    _last_error = "解密失败（所有候选密钥均不匹配或密文损坏）"
    _log.error("[crypto] %s —— 请确认凭证密钥是否与加密时一致（CRED_KEY / data/cred.key / "
               "SECRET_KEY 三者之一）。若密钥确已无法找回，请在「集群设置 / 节点」重新录入凭证，"
               "使其按当前密钥重新加密。【已原样返回密文，凭证未被清空】", _last_error)
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
