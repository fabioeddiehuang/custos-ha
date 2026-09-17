"""pytest 公共夹具。

设计目标：让 Custos-HA 的**纯逻辑**能在不依赖真实 MySQL/MariaDB/keepalived 的情况下被单测。

- 在任何 ``import app`` 之前固化测试环境（强 SECRET_KEY、凭证加密默认关闭、DATA_DIR 指向临时目录）。
- 提供可脚本化的 ``FakeCursor`` / ``FakeConnection``：按 SQL 子串匹配返回预设结果，并记录执行过的 SQL。
- ``autouse`` 夹具把 ``app.db.get_setting`` 桩成返回 None，避免任何测试触达真实管理库，
  并让 ``crypto.enabled()`` / ``config.is_semi_sync_enabled()`` 等稳定回退到 config 环境变量。
"""
import os
import sys
from pathlib import Path

# ---- 0. 在任何 app 导入前固化测试环境 ----
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-unit-tests-1234567890abcd")
os.environ.setdefault("CRED_ENCRYPTION", "off")
_DATA_DIR = "/tmp/custos-test-data"
os.environ.setdefault("DATA_DIR", _DATA_DIR)
os.makedirs(_DATA_DIR, exist_ok=True)
os.environ.setdefault("MGMT_MYSQL_HOST", "127.0.0.1")
os.environ.setdefault("MGMT_MYSQL_PORT", "3306")
os.environ.setdefault("MGMT_MYSQL_USER", "ha")
os.environ.setdefault("MGMT_MYSQL_PASS", "ha")
os.environ.setdefault("MGMT_MYSQL_DB", "ha")

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest  # noqa: E402


class FakeCursor:
    """可脚本化的 PyMySQL 游标替身。

    - ``responses``：{子串: 行列表}；``execute(sql)`` 按首个命中子串决定本次返回行。
    - 支持 ``with`` 上下文、``fetchone`` / ``fetchall``；``executed`` 记录所有执行过的 SQL 便于断言。
    """

    def __init__(self, responses=None):
        self.responses = dict(responses or {})
        self.executed = []
        self._rows = []
        self.rowcount = 0
        self.lastrowid = 0

    def add(self, sql_substr, rows):
        """注册：当执行的 SQL 包含 ``sql_substr`` 时返回 ``rows``。"""
        self.responses[sql_substr] = rows
        return self

    def execute(self, sql, args=None):
        self.executed.append(sql)
        self._rows = []
        low = (sql or "").lower()
        for key, rows in self.responses.items():
            if key.lower() in low:
                self._rows = rows
                break
        self.rowcount = len(self._rows)
        return None

    def executemany(self, sql, args=None):
        self.executed.append(sql)
        return None

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def close(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConnection:
    """可脚本化的 PyMySQL 连接替身；``cursor()`` 复用同一 ``FakeCursor``。"""

    def __init__(self, responses=None):
        self._cur = FakeCursor(responses)
        self.closed = False

    def cursor(self):
        return self._cur

    def commit(self):
        return None

    def close(self):
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


@pytest.fixture
def fake_cursor():
    return FakeCursor()


@pytest.fixture
def fake_connection():
    return FakeConnection()


@pytest.fixture(autouse=True)
def _isolate_db_get_setting(monkeypatch):
    """桩掉真实管理库读取，所有测试都不应触达真实 MySQL。"""
    import app.db
    monkeypatch.setattr(app.db, "get_setting", lambda key, default=None: None)
    yield


@pytest.fixture
def fresh_crypto():
    """重置 crypto 模块级缓存，保证加密相关测试彼此隔离。"""
    from app import crypto
    crypto._fernet = None
    crypto._last_error = ""
    yield
    crypto._fernet = None
    crypto._last_error = ""
