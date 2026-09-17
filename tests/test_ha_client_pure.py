"""ha_client 纯逻辑单测：GTID 解析、1236 清理错误识别、残留主库判定、
以及 make_slave_of 的「ENC1 密文入口守卫」（v2.3.10 修复的 1470 事故防护）。

make_slave_of 的守卫必须在**连库之前**拦截未解密的复制口令，否则密文会被拼进
CHANGE MASTER 报 1470。这里验证守卫确实短路、且不发起任何连接。
"""
from app import ha_client


def test_parse_gtid_set():
    s = ha_client.parse_gtid_set("uuid1:1-33,uuid2:1-19")
    assert s == {"uuid1": set(range(1, 34)), "uuid2": set(range(1, 20))}
    assert ha_client.parse_gtid_set("") == {}
    assert ha_client.parse_gtid_set(None) == {}
    # MySQL 版解析器不认 MariaDB 三段格式，应跳过而非崩溃
    assert ha_client.parse_gtid_set("0-1-100") == {}


def test_is_gtid_purged_error():
    assert ha_client.is_gtid_purged_error(
        "Got fatal error 1236 ... Could not find GTID state requested by slave") is True
    assert ha_client.is_gtid_purged_error("required binlog files have been purged") is True
    assert ha_client.is_gtid_purged_error("slave state is too old") is True
    assert ha_client.is_gtid_purged_error("1236 ... purged") is True
    # 非清理类 1236 不应误判为「需重建」
    assert ha_client.is_gtid_purged_error("1236 something else") is False
    # 凭证类错误不是清理类
    assert ha_client.is_gtid_purged_error("Access denied for user") is False
    assert ha_client.is_gtid_purged_error("") is False
    assert ha_client.is_gtid_purged_error(None) is False


def test_is_stale_master():
    assert ha_client.is_stale_master({"mode": "master", "master_host": None}) is True
    assert ha_client.is_stale_master({"mode": "master", "master_host": "10.0.0.2"}) is False
    assert ha_client.is_stale_master({"mode": "slave"}) is False
    assert ha_client.is_stale_master(None) is False
    assert ha_client.is_stale_master({}) is False


def test_make_slave_of_refuses_encrypted_repl_pass_without_connecting(monkeypatch):
    calls = []
    def fake_connect(*a, **k):
        calls.append((a, k))
        return None
    monkeypatch.setattr(ha_client, "connect_robust", fake_connect)

    ok, msg = ha_client.make_slave_of(
        "10.0.0.1", 3306, "ha_monitor", "ha_pass",
        "10.0.0.2", 3306, "replication", "ENC1:deadbeefciphertext",
    )
    assert ok is False
    assert "ENC1" in msg
    assert calls == []   # 绝不能发起连接，避免把密文写进复制配置


def test_make_slave_of_proceeds_to_connect_with_plain_repl_pass(monkeypatch):
    calls = []
    def fake_connect(*a, **k):
        calls.append((a, k))
        return None
    monkeypatch.setattr(ha_client, "connect_robust", fake_connect)

    ok, msg = ha_client.make_slave_of(
        "10.0.0.1", 3306, "ha_monitor", "ha_pass",
        "10.0.0.2", 3306, "replication", "plain_repl_pass",
    )
    assert calls, "明文复制口令应进入连库分支"
    assert ok is False
    assert "无法连接" in msg
