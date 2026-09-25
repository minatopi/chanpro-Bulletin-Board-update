import os
import sys
import time
import ssl
import threading
from datetime import datetime, timezone

import psycopg
import paho.mqtt.client as mqtt


# ============================================================
# 設定
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL が設定されていません。")
    sys.exit(1)


MQTT_HOST = os.environ.get(
    "MQTT_HOST",
    "broker.hivemq.com"
)

MQTT_PORT = int(
    os.environ.get(
        "MQTT_PORT",
        "8883"
    )
)

MQTT_USERNAME = os.environ.get(
    "MQTT_USERNAME",
    ""
)

MQTT_PASSWORD = os.environ.get(
    "MQTT_PASSWORD",
    ""
)


MQTT_BASE = "chanpro-share-v2"

MQTT_FILE_BASE = (
    MQTT_BASE + "/file"
)


# ============================================================
# 待ち時間設定
# ============================================================

# MQTT接続待ち
MQTT_CONNECT_TIMEOUT = 60

# MQTT Publish完了待ち
#
# ここを10分に設定
#
# 大容量ファイルや大量チャンクでも
# かなり余裕を持って待つ
#
MQTT_PUBLISH_TIMEOUT = 600


# Publishリトライ回数
PUBLISH_RETRY_COUNT = 5


# リトライ間隔
PUBLISH_RETRY_DELAY = 5


# Retainデータ取得待ち
RETAIN_RECEIVE_TIMEOUT = 300


# データ受信後の安定待ち
RETAIN_SETTLE_TIME = 3


# MQTT再接続待ち
MQTT_RECONNECT_DELAY = 10


# 最大ファイル数
MAX_FILES = 100


# ============================================================
# MQTTデータ格納
# ============================================================

received_messages = {}

received_lock = threading.Lock()

connected_event = threading.Event()


# ============================================================
# MQTT Client
# ============================================================

mqtt_client = None


# ============================================================
# MQTT CONNECT
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    rc,
    properties=None
):
    if rc == 0:
        print(
            "[MQTT] Connected successfully"
        )

        connected_event.set()

    else:
        print(
            f"[MQTT] Connect failed rc={rc}"
        )


# ============================================================
# MQTT DISCONNECT
# ============================================================

def on_disconnect(
    client,
    userdata,
    rc,
    properties=None
):
    connected_event.clear()

    print(
        f"[MQTT] Disconnected rc={rc}"
    )


# ============================================================
# MQTT MESSAGE
# ============================================================

def on_message(
    client,
    userdata,
    msg
):
    topic = msg.topic

    payload = bytes(msg.payload)

    with received_lock:
        received_messages[topic] = payload

    print(
        f"[MQTT] Received retained: "
        f"{topic} "
        f"({len(payload):,} bytes)"
    )


# ============================================================
# MQTT初期化
# ============================================================

def create_mqtt_client():

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
        client_id=(
            "chanpro-github-refresh-"
            + str(int(time.time()))
        )
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    # TLS
    client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED
    )

    client.tls_insecure_set(False)

    # 認証情報
    if MQTT_USERNAME:
        client.username_pw_set(
            MQTT_USERNAME,
            MQTT_PASSWORD
        )

    return client


# ============================================================
# MQTT接続
# ============================================================

def connect_mqtt():

    global mqtt_client

    mqtt_client = create_mqtt_client()

    print(
        f"[MQTT] Connecting "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    mqtt_client.connect(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=120
    )

    mqtt_client.loop_start()

    start = time.time()

    while True:

        if connected_event.is_set():
            print("[MQTT] Connection ready")
            return

        if time.time() - start >= MQTT_CONNECT_TIMEOUT:
            raise TimeoutError(
                "MQTT接続がタイムアウトしました"
            )

        time.sleep(0.2)


# ============================================================
# MQTT接続確認
# ============================================================

def ensure_mqtt_connection():

    if mqtt_client is None:
        connect_mqtt()
        return

    if connected_event.is_set():
        return

    print(
        "[MQTT] 再接続を開始します..."
    )

    time.sleep(
        MQTT_RECONNECT_DELAY
    )

    try:
        mqtt_client.reconnect()

    except Exception as e:
        print(
            f"[MQTT] reconnect error: {e}"
        )

    start = time.time()

    while not connected_event.is_set():

        if (
            time.time() - start
            >= MQTT_CONNECT_TIMEOUT
        ):
            raise TimeoutError(
                "MQTT再接続がタイムアウトしました"
            )

        time.sleep(0.5)


# ============================================================
# Publish
# ============================================================

def publish_with_retry(
    topic,
    payload,
    retain=True,
    qos=0
):

    last_error = None

    for attempt in range(
        1,
        PUBLISH_RETRY_COUNT + 1
    ):

        print(
            f"[MQTT] Publish "
            f"{attempt}/{PUBLISH_RETRY_COUNT}: "
            f"{topic} "
            f"({len(payload):,} bytes)"
        )

        try:

            ensure_mqtt_connection()

            info = mqtt_client.publish(
                topic,
                payload=payload,
                qos=qos,
                retain=retain
            )

            # ------------------------------------------------
            # Publish完了待ち
            #
            # 最大10分
            # ------------------------------------------------

            info.wait_for_publish(
                timeout=MQTT_PUBLISH_TIMEOUT
            )

            # 完了確認
            if not info.is_published():

                raise TimeoutError(
                    "MQTT Publish完了待ちが"
                    "タイムアウトしました"
                )

            print(
                f"[MQTT] Publish completed: "
                f"{topic}"
            )

            return True

        except Exception as e:

            last_error = e

            print(
                f"[MQTT] Publish failed: "
                f"{topic}"
            )

            print(
                f"        {e}"
            )

            if attempt < PUBLISH_RETRY_COUNT:

                print(
                    f"[MQTT] "
                    f"{PUBLISH_RETRY_DELAY}秒後に"
                    f"リトライします"
                )

                time.sleep(
                    PUBLISH_RETRY_DELAY
                )

                try:
                    ensure_mqtt_connection()

                except Exception as reconnect_error:

                    print(
                        "[MQTT] "
                        "再接続エラー: "
                        f"{reconnect_error}"
                    )

    raise RuntimeError(
        f"MQTT Publish failed: "
        f"{topic}: {last_error}"
    )


# ============================================================
# Supabase / PostgreSQL
# ============================================================

def get_shared_files():

    print(
        "[DB] shared_files を取得しています..."
    )

    sql = """
        SELECT
            id,
            user_id,
            file_name,
            file_size,
            mime_type,
            chunk_count,
            chunk_size,
            mqtt_topic,
            created_at
        FROM public.shared_files
        ORDER BY created_at DESC
        LIMIT %s
    """

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(
                sql,
                (MAX_FILES,)
            )

            rows = cur.fetchall()

    files = []

    for row in rows:

        files.append({
            "id": row[0],
            "user_id": row[1],
            "file_name": row[2],
            "file_size": row[3],
            "mime_type": row[4],
            "chunk_count": row[5],
            "chunk_size": row[6],
            "mqtt_topic": row[7],
            "created_at": row[8],
        })

    print(
        f"[DB] {len(files)}件取得しました"
    )

    return files


# ============================================================
# MQTT Retain受信
# ============================================================

def receive_retained_file(file_info):

    topic = file_info["mqtt_topic"]

    chunk_count = int(
        file_info["chunk_count"]
    )

    print("")
    print("=" * 70)
    print(
        f"[RECEIVE] {file_info['file_name']}"
    )
    print(
        f"[RECEIVE] chunks={chunk_count}"
    )
    print(
        f"[RECEIVE] topic={topic}"
    )
    print("=" * 70)

    with received_lock:
        received_messages.clear()

    ensure_mqtt_connection()

    chunk_topic = (
        topic.rstrip("/")
        + "/chunk/+"
    )

    meta_topic = (
        topic.rstrip("/")
        + "/meta"
    )

    # --------------------------------------------------------
    # 一度だけsubscribe
    # --------------------------------------------------------

    print(
        f"[MQTT] Subscribe: {chunk_topic}"
    )

    mqtt_client.subscribe(
        chunk_topic,
        qos=0
    )

    mqtt_client.subscribe(
        meta_topic,
        qos=0
    )

    expected_chunks = set()

    for index in range(chunk_count):

        expected_chunks.add(
            f"{topic}/chunk/{index:06d}"
        )

    expected_meta = meta_topic

    start = time.time()

    last_count = 0
    last_change = time.time()

    while True:

        with received_lock:
            current_topics = set(
                received_messages.keys()
            )

        received_chunk_topics = (
            current_topics
            & expected_chunks
        )

        count = len(
            received_chunk_topics
        )

        if count != last_count:

            print(
                f"[RECEIVE] "
                f"{count}/{chunk_count} "
                f"chunks"
            )

            last_count = count
            last_change = time.time()

        # 全チャンク取得
        if (
            count >= chunk_count
            and expected_meta in current_topics
        ):

            print(
                "[RECEIVE] "
                "全チャンク受信完了"
            )

            break

        # 最大5分
        if (
            time.time() - start
            >= RETAIN_RECEIVE_TIMEOUT
        ):

            missing = (
                expected_chunks
                - received_chunk_topics
            )

            raise TimeoutError(
                "Retainデータ受信タイムアウト: "
                f"{len(missing)} chunks missing"
            )

        time.sleep(0.2)

    # --------------------------------------------------------
    # 追加安定待ち
    # --------------------------------------------------------

    print(
        f"[RECEIVE] "
        f"{RETAIN_SETTLE_TIME}秒安定待ち..."
    )

    time.sleep(
        RETAIN_SETTLE_TIME
    )

    with received_lock:

        data = {}

        for chunk_topic_name in (
            expected_chunks
        ):

            if (
                chunk_topic_name
                not in received_messages
            ):
                raise RuntimeError(
                    "チャンクが不足しています: "
                    f"{chunk_topic_name}"
                )

            data[
                chunk_topic_name
            ] = received_messages[
                chunk_topic_name
            ]

        meta = received_messages.get(
            expected_meta
        )

    # --------------------------------------------------------
    # unsubscribe
    # --------------------------------------------------------

    try:

        mqtt_client.unsubscribe(
            chunk_topic
        )

        mqtt_client.unsubscribe(
            meta_topic
        )

    except Exception:
        pass

    return meta, data


# ============================================================
# MQTT Retain再Publish
# ============================================================

def refresh_file(file_info):

    file_name = file_info["file_name"]

    print("")
    print("#" * 70)
    print(
        f"[FILE] {file_name}"
    )
    print(
        f"[FILE] size="
        f"{int(file_info['file_size']):,} bytes"
    )
    print(
        f"[FILE] chunks="
        f"{file_info['chunk_count']}"
    )
    print("#" * 70)

    # --------------------------------------------------------
    # 既存Retainを取得
    # --------------------------------------------------------

    meta, chunks = (
        receive_retained_file(
            file_info
        )
    )

    topic = file_info["mqtt_topic"]

    # --------------------------------------------------------
    # metaを再Publish
    # --------------------------------------------------------

    print(
        "[PUBLISH] metadata"
    )

    publish_with_retry(
        topic=f"{topic}/meta",
        payload=meta,
        retain=True,
        qos=0
    )

    # --------------------------------------------------------
    # chunkを再Publish
    # --------------------------------------------------------

    chunk_count = int(
        file_info["chunk_count"]
    )

    for index in range(
        chunk_count
    ):

        chunk_topic = (
            f"{topic}/chunk/"
            f"{index:06d}"
        )

        payload = chunks[
            chunk_topic
        ]

        print(
            f"[PUBLISH] "
            f"{index + 1}/{chunk_count} "
            f"{file_name}"
        )

        publish_with_retry(
            topic=chunk_topic,
            payload=payload,
            retain=True,
            qos=0
        )

    print("")
    print(
        f"[FILE] 再保存完了: "
        f"{file_name}"
    )


# ============================================================
# DB更新
# ============================================================

def update_refresh_time(
    file_id
):

    sql = """
        ALTER TABLE public.shared_files
        ADD COLUMN IF NOT EXISTS
        mqtt_last_refreshed_at
        timestamptz
    """

    update_sql = """
        UPDATE public.shared_files
        SET mqtt_last_refreshed_at = NOW()
        WHERE id = %s
    """

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(sql)

            cur.execute(
                update_sql,
                (file_id,)
            )

        conn.commit()


# ============================================================
# メイン
# ============================================================

def main():

    print("")
    print("=" * 70)
    print(
        "ChanPro Shared Files MQTT Refresh"
    )
    print("=" * 70)

    print(
        f"MQTT broker : "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    print(
        f"Publish timeout : "
        f"{MQTT_PUBLISH_TIMEOUT}秒"
    )

    print(
        f"Publish retry : "
        f"{PUBLISH_RETRY_COUNT}回"
    )

    print(
        f"Receive timeout : "
        f"{RETAIN_RECEIVE_TIMEOUT}秒"
    )

    print("=" * 70)
    print("")

    files = get_shared_files()

    if not files:

        print(
            "[MAIN] 対象ファイルはありません"
        )

        return

    # --------------------------------------------------------
    # MQTT接続
    # --------------------------------------------------------

    connect_mqtt()

    success_count = 0
    failure_count = 0

    errors = []

    # --------------------------------------------------------
    # ファイル処理
    # --------------------------------------------------------

    for index, file_info in enumerate(
        files,
        start=1
    ):

        print("")
        print(
            f"[MAIN] "
            f"FILE {index}/{len(files)}"
        )

        try:

            refresh_file(
                file_info
            )

            try:

                update_refresh_time(
                    file_info["id"]
                )

            except Exception as e:

                print(
                    "[DB] "
                    "refresh time更新失敗: "
                    f"{e}"
                )

            success_count += 1

        except Exception as e:

            failure_count += 1

            error_message = (
                f"{file_info['file_name']}: "
                f"{e}"
            )

            errors.append(
                error_message
            )

            print(
                "[ERROR] "
                f"{error_message}"
            )

            # ------------------------------------------------
            # 1ファイル失敗しても
            # 次のファイルへ進む
            # ------------------------------------------------

            continue

    # --------------------------------------------------------
    # MQTT停止
    # --------------------------------------------------------

    if mqtt_client:

        try:

            mqtt_client.loop_stop()

        except Exception:
            pass

        try:

            mqtt_client.disconnect()

        except Exception:
            pass

    # --------------------------------------------------------
    # 結果
    # --------------------------------------------------------

    print("")
    print("=" * 70)
    print("処理結果")
    print("=" * 70)

    print(
        f"成功: {success_count}"
    )

    print(
        f"失敗: {failure_count}"
    )

    print(
        f"全体: {len(files)}"
    )

    if errors:

        print("")
        print(
            "失敗したファイル:"
        )

        for error in errors:

            print(
                f" - {error}"
            )

    print("=" * 70)

    # --------------------------------------------------------
    # 失敗があった場合
    # GitHub Actionsを失敗扱い
    # --------------------------------------------------------

    if failure_count > 0:

        raise RuntimeError(
            f"{failure_count}件の"
            "ファイルで処理に失敗しました"
        )

    print("")
    print(
        "すべてのMQTT Retain再保存が"
        "正常に完了しました。"
    )


# ============================================================
# 実行
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            "処理を中断しました。"
        )

        sys.exit(130)

    except Exception as e:

        print("")
        print(
            "================================"
        )

        print(
            "ERROR:"
        )

        print(
            str(e)
        )

        print(
            "================================"
        )

        sys.exit(1)
