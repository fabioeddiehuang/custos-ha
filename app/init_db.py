"""初始化管理库表结构与管理员账号。"""
import hashlib
import secrets
from .db import init_schema, get_conn, startup_credential_check
from .config import WEB_USER, WEB_PASS, hash_pwd, WEB_PORT
from .selfcheck import selfcheck_manager
from .preflight import preflight_install


def _boot_gates():
    """启动期双闸门：自检 + 安装预检（Phase 3，稳定+安全）。

    任一不过都**显式打印**并给出修复指引，绝不静默带病运行。
    预检默认不阻断启动（避免磁盘瞬时告警把管理端打死），但 error 级会醒目提示。
    """
    try:
        issues = selfcheck_manager()
        for level, msg in issues:
            if level == "error":
                print("[selfcheck][ERROR] %s" % msg)
            elif level == "warn":
                print("[selfcheck][WARN] %s" % msg)
            else:
                print("[selfcheck][OK] %s" % msg)
    except Exception as e:
        print("[selfcheck] 自检异常(可忽略): %s" % e)

    try:
        import os
        from .config import DATA_DIR
        spec = {
            "device_free_gb_min": float(os.environ.get("PREFLIGHT_FREE_GB_MIN", "5")),
            "ports": [int(WEB_PORT)],
            # 离线优先策略：显式要求镜像时未载入才报错；否则仅提示
            "image_loaded": None if os.environ.get("OFFLINE_IMAGES_REQUIRED") == "1"
                            else True,
            "flavour_supported": True,
        }
        ok, pf = preflight_install(spec)
        for level, msg in pf:
            if level == "error":
                print("[preflight][ERROR] %s" % msg)
            elif level == "warn":
                print("[preflight][WARN] %s" % msg)
            else:
                print("[preflight][OK] %s" % msg)
        if not ok:
            print("[preflight] 预检未通过：请按上述指引修复后再继续。")
    except Exception as e:
        print("[preflight] 预检异常(可忽略): %s" % e)


def main():
    _boot_gates()
    init_schema()
    # v2.3.10：启动即自检凭证可解密性。存在「解不开」的凭证必须立刻告警，
    # 否则要等到复制建立时密文被当口令报 1470 才会暴露（现象与密钥毫无关联，极难定位）。
    try:
        startup_credential_check()
    except Exception as e:
        print(f"[init_db] 凭证自检异常(可忽略): {e}")
    conn = get_conn()
    try:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) AS n FROM users WHERE username=%s", (WEB_USER,))
            exists = c.fetchone()["n"]

            if WEB_PASS:
                # 显式指定密码：以 compose / 环境变量为准，每次启动确保账号密码一致，
                # 因此可通过修改 WEB_PASS 来改密码（覆盖既有账号）。
                pwd = WEB_PASS
                h = hash_pwd(pwd)
                if exists:
                    c.execute("UPDATE users SET password_hash=%s WHERE username=%s",
                              (h, WEB_USER))
                else:
                    c.execute("INSERT INTO users (username, password_hash) VALUES (%s,%s)",
                              (WEB_USER, h))
                conn.commit()
                print("=" * 50)
                print(f"  管理员账号 : {WEB_USER}")
                print(f"  管理员密码 : {pwd}   (来自 WEB_PASS 环境变量)")
                print("=" * 50)
            else:
                # 未指定密码
                if exists:
                    # 账号已存在：保持既有密码，仅提示如何重置
                    print("=" * 50)
                    print(f"  管理员账号 : {WEB_USER} (已存在)")
                    print("  密码沿用既有设置；如需重置：在 Web「设置」中修改，")
                    print("  或于 compose / 环境变量设置 WEB_PASS 后重启本容器。")
                    print("=" * 50)
                else:
                    # 首次启动且未指定密码：随机生成并打印
                    pwd = secrets.token_urlsafe(12)
                    c.execute("INSERT INTO users (username, password_hash) VALUES (%s,%s)",
                              (WEB_USER, hash_pwd(pwd)))
                    conn.commit()
                    print("=" * 50)
                    print(f"  初始管理员账号 : {WEB_USER}")
                    print(f"  初始管理员密码 : {pwd}   (请尽快在设置中修改)")
                    print("=" * 50)
    finally:
        conn.close()
    print("[init_db] 初始化完成")


if __name__ == "__main__":
    main()
