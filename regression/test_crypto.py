"""crypto 单测：凭证静态加密的加解密闭环、ENC1 守卫、密钥不匹配时「原样返回密文」保底。

这些正是 v2.3.10 那次「复制口令变成 ENC1 密文 → 报 1470 → 复制集体瘫痪」事故的防护点。
重点是：解不开时**绝不返回空串**（否则真实口令被永久覆盖），而是原样返回密文让故障暴露。
"""
import app.config as config
from cryptography.fernet import Fernet
from app import crypto


def test_is_encrypted():
    assert crypto.is_encrypted("ENC1:abc") is True
    assert crypto.is_encrypted("plain") is False
    assert crypto.is_encrypted("") is False
    assert crypto.is_encrypted(None) is False


def test_roundtrip_when_enabled(fresh_crypto, monkeypatch):
    key = Fernet.generate_key()
    monkeypatch.setenv("CRED_KEY", key.decode())
    monkeypatch.setattr(config, "CRED_ENCRYPTION", "on")
    ct = crypto.encrypt("my-secret-pw")
    assert ct.startswith("ENC1:")
    assert crypto.decrypt(ct) == "my-secret-pw"


def test_disabled_stores_plaintext(fresh_crypto, monkeypatch):
    monkeypatch.setattr(config, "CRED_ENCRYPTION", "off")
    assert crypto.encrypt("my-secret-pw") == "my-secret-pw"
    assert crypto.decrypt("my-secret-pw") == "my-secret-pw"


def test_empty_and_none_passthrough(fresh_crypto):
    assert crypto.encrypt("") == ""
    assert crypto.encrypt(None) is None
    assert crypto.decrypt("") == ""


def test_idempotent_on_ciphertext(fresh_crypto, monkeypatch):
    key = Fernet.generate_key()
    monkeypatch.setenv("CRED_KEY", key.decode())
    monkeypatch.setattr(config, "CRED_ENCRYPTION", "on")
    ct = crypto.encrypt("pw")
    assert crypto.encrypt(ct) == ct  # 已是密文不再重复加密


def test_key_mismatch_returns_ciphertext_not_empty(fresh_crypto, monkeypatch):
    key_a = Fernet.generate_key()
    monkeypatch.setenv("CRED_KEY", key_a.decode())
    monkeypatch.setattr(config, "CRED_ENCRYPTION", "on")
    ct = crypto.encrypt("pw")

    # 换成完全不同的密钥：环境 + 持久文件/保险库候选均指向不存在路径，杜绝命中 key_a
    key_b = Fernet.generate_key()
    monkeypatch.setenv("CRED_KEY", key_b.decode())
    monkeypatch.setattr(crypto, "_key_path", lambda: "/tmp/custos-test-nonexistent-key-a")
    monkeypatch.setattr(crypto, "_vault_path", lambda: "/tmp/custos-test-nonexistent-key-b")
    crypto._fernet = None

    out = crypto.decrypt(ct)
    # 解不开 → 原样返回密文（绝不返回空串/明文），让上层按 1470 暴露密钥问题
    assert out == ct
    assert crypto.is_encrypted(out) is True
