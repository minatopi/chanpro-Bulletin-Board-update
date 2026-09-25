import os
import sys
import time
import ssl
import uuid
import threading
import json

import psycopg
import paho.mqtt.client as mqtt


# ============================================================
# 設定
# ============================================================

DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL が設定されていません")
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
MQTT_FILE_BASE = MQTT_BASE + "/file"


# ============================================================
# 待ち時間
# ============================================================

# MQTT接続待ち
MQTT_CONNECT_TIMEOUT = 60


# Publish完了待ち
# 1回につき最大10分
MQTT_PUBLISH_TIMEOUT = 600


# Publishリトライ
PUBLISH_RETRY_COUNT = 5


# リトライ間隔
PUBLISH_RETRY_DELAY = 5


# 元データの読み込み待ち
RETAIN_RECEIVE_TIMEOUT = 300


# 全チャンク取得後の安定待ち
RETAIN_SETTLE_TIME = 3


# 最大ファイル数
MAX_FILES = 100


# ============================================================
# MQTT状態
# ============================================================

mqtt_client = None

connected_event = threading.Event()

received_messages = {}

received_lock = threading.Lock()


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
            "[MQTT] Connected"
        )

        connected_event.set()

    else:

        print(
            f"[MQTT] Connect failed: rc={rc}"
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
        f"[MQTT] Disconnected: rc={rc}"
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
        "[MQTT] received:",
        topic,
        f"({len(payload):,} bytes)"
    )


# ============================================================
# MQTT Client作成
# ============================================================

def create_mqtt_client():

    client = mqtt.Client(
        callback_api_version=
            mqtt.CallbackAPIVersion.VERSION1,

        client_id=(
            "chanpro-share-rebuild-"
            + str(uuid.uuid4())
        )
    )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.tls_set(
        cert_reqs=ssl.CERT_REQUIRED
    )

    client.tls_insecure_set(False)

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

    start_time = time.time()

    while True:

        if connected_event.is_set():

            print(
                "[MQTT] Connection ready"
            )

            return

        if (
            time.time() - start_time
            >= MQTT_CONNECT_TIMEOUT
        ):

            raise TimeoutError(
                "MQTT接続タイムアウト"
            )

        time.sleep(0.2)


# ============================================================
# MQTT接続確認
# ============================================================

def ensure_mqtt_connection():

    if (
        mqtt_client is not None
        and connected_event.is_set()
    ):

        return

    print(
        "[MQTT] 再接続します"
    )

    try:

        mqtt_client.reconnect()

    except Exception as e:

        print(
            "[MQTT] reconnect error:",
            e
        )

    start_time = time.time()

    while not connected_event.is_set():

        if (
            time.time() - start_time
            >= MQTT_CONNECT_TIMEOUT
        ):

            raise TimeoutError(
                "MQTT再接続タイムアウト"
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
            f"{attempt}/{PUBLISH_RETRY_COUNT}"
        )

        print(
            f"        {topic}"
        )

        try:

            ensure_mqtt_connection()

            info = mqtt_client.publish(
                topic,
                payload=payload,
                qos=qos,
                retain=retain
            )

            # =================================================
            # Publish完了待ち
            # 最大10分
            # =================================================

            info.wait_for_publish(
                timeout=MQTT_PUBLISH_TIMEOUT
            )

            if not info.is_published():

                raise TimeoutError(
                    "MQTT Publish完了待ち"
                    "タイムアウト"
                )

            print(
                "[MQTT] Publish completed"
            )

            return True

        except Exception as e:

            last_error = e

            print(
                "[MQTT] Publish failed:",
                e
            )

            if (
                attempt
                < PUBLISH_RETRY_COUNT
            ):

                print(
                    f"{PUBLISH_RETRY_DELAY}秒後に"
                    "リトライします"
                )

                time.sleep(
                    PUBLISH_RETRY_DELAY
                )

    raise RuntimeError(
        f"Publish failed: {topic}: "
        f"{last_error}"
    )


# ============================================================
# DBからファイル一覧取得
# ============================================================

def get_shared_files():

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
        ORDER BY created_at ASC
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

            "created_at": row[8]
        })

    return files


# ============================================================
# 元MQTTデータ読み込み
# ============================================================

def read_old_mqtt_data(
    file_info
):

    old_topic = file_info["mqtt_topic"]

    chunk_count = int(
        file_info["chunk_count"]
    )

    print("")
    print(
        "=" * 70
    )

    print(
        "[READ OLD]"
    )

    print(
        "file:",
        file_info["file_name"]
    )

    print(
        "old topic:",
        old_topic
    )

    print(
        "chunks:",
        chunk_count
    )

    print(
        "=" * 70
    )

    with received_lock:

        received_messages.clear()

    ensure_mqtt_connection()

    chunk_topic = (
        old_topic
        + "/chunk/+"
    )

    meta_topic = (
        old_topic
        + "/meta"
    )

    # --------------------------------------------------------
    # subscribe
    # --------------------------------------------------------

    mqtt_client.subscribe(
        chunk_topic,
        qos=0
    )

    mqtt_client.subscribe(
        meta_topic,
        qos=0
    )

    expected_chunks = set()

    for index in range(
        chunk_count
    ):

        expected_chunks.add(
            f"{old_topic}/chunk/"
            f"{index:06d}"
        )

    start_time = time.time()

    last_count = -1

    while True:

        with received_lock:

            current = set(
                received_messages.keys()
            )

        received_chunks = (
            current
            & expected_chunks
        )

        count = len(
            received_chunks
        )

        if count != last_count:

            print(
                f"[READ OLD] "
                f"{count}/{chunk_count}"
            )

            last_count = count

        # 全チャンク＋meta
        if (
            count == chunk_count
            and meta_topic in current
        ):

            print(
                "[READ OLD] "
                "読み込み完了"
            )

            break

        if (
            time.time() - start_time
            >= RETAIN_RECEIVE_TIMEOUT
        ):

            missing = (
                expected_chunks
                - received_chunks
            )

            raise TimeoutError(
                "元MQTTデータの読み込み"
                "タイムアウト: "
                f"{len(missing)} chunks missing"
            )

        time.sleep(0.2)

    # --------------------------------------------------------
    # 安定待ち
    # --------------------------------------------------------

    time.sleep(
        RETAIN_SETTLE_TIME
    )

    with received_lock:

        meta = received_messages[
            meta_topic
        ]

        chunks = {}

        for index in range(
            chunk_count
        ):

            topic = (
                f"{old_topic}/chunk/"
                f"{index:06d}"
            )

            if topic not in received_messages:

                raise RuntimeError(
                    "チャンク不足: "
                    + topic
                )

            chunks[index] = (
                received_messages[topic]
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

    return meta, chunks


# ============================================================
# 新しいID生成
# ============================================================

def create_new_file_id():

    return str(
        uuid.uuid4()
    )


# ============================================================
# 新しいMQTT topic
# ============================================================

def create_new_topic(
    new_id
):

    return (
        MQTT_FILE_BASE
        + "/"
        + new_id
    )


# ============================================================
# 新しいMQTTデータ作成
# ============================================================

def create_new_mqtt_data(
    old_file,
    old_meta,
    chunks,
    new_id,
    new_topic
):

    # --------------------------------------------------------
    # metaを書き換える
    # --------------------------------------------------------

    new_meta = old_meta

    try:

        decoded = json.loads(
            old_meta.decode("utf-8")
        )

        if isinstance(
            decoded,
            dict
        ):

            decoded["id"] = new_id

            decoded["file_id"] = new_id

            decoded["mqtt_topic"] = (
                new_topic
            )

            decoded["rebuilt_from"] = (
                str(old_file["id"])
            )

            decoded["rebuilt_at"] = (
                time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime()
                )
            )

            new_meta = json.dumps(
                decoded,
                ensure_ascii=False,
                separators=(",", ":")
            ).encode("utf-8")

    except Exception:

        # JSONでない場合は
        # 元metaをそのまま使用
        new_meta = old_meta

    return new_meta


# ============================================================
# 新しいMQTTへ作成
# ============================================================

def publish_new_file(
    file_info,
    meta,
    chunks,
    new_id,
    new_topic
):

    print("")
    print(
        "=" * 70
    )

    print(
        "[CREATE NEW]"
    )

    print(
        "old ID:",
        file_info["id"]
    )

    print(
        "new ID:",
        new_id
    )

    print(
        "new topic:",
        new_topic
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # meta
    # --------------------------------------------------------

    new_meta = create_new_mqtt_data(
        file_info,
        meta,
        chunks,
        new_id,
        new_topic
    )

    publish_with_retry(
        topic=(
            new_topic
            + "/meta"
        ),
        payload=new_meta,
        retain=True,
        qos=0
    )

    # --------------------------------------------------------
    # chunks
    # --------------------------------------------------------

    chunk_count = int(
        file_info["chunk_count"]
    )

    for index in range(
        chunk_count
    ):

        new_chunk_topic = (
            f"{new_topic}/chunk/"
            f"{index:06d}"
        )

        payload = chunks[index]

        print(
            f"[CREATE NEW] "
            f"chunk "
            f"{index + 1}/{chunk_count}"
        )

        publish_with_retry(
            topic=new_chunk_topic,
            payload=payload,
            retain=True,
            qos=0
        )

    print(
        "[CREATE NEW] "
        "MQTT作成完了"
    )


# ============================================================
# 新しいDBレコード作成
# ============================================================

def insert_new_db_record(
    old_file,
    new_id,
    new_topic
):

    sql = """
        INSERT INTO public.shared_files (
            id,
            user_id,
            file_name,
            file_size,
            mime_type,
            chunk_count,
            chunk_size,
            mqtt_topic,
            created_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            NOW()
        )
    """

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(
                sql,
                (
                    new_id,
                    old_file["user_id"],
                    old_file["file_name"],
                    old_file["file_size"],
                    old_file["mime_type"],
                    old_file["chunk_count"],
                    old_file["chunk_size"],
                    new_topic
                )
            )

        conn.commit()

    print(
        "[DB] 新しいレコード作成完了:",
        new_id
    )


# ============================================================
# 新MQTTデータ存在確認
# ============================================================

def verify_new_mqtt_data(
    file_info,
    new_id,
    new_topic
):

    print(
        "[VERIFY] "
        "新しいMQTTデータを確認します"
    )

    with received_lock:

        received_messages.clear()

    ensure_mqtt_connection()

    chunk_count = int(
        file_info["chunk_count"]
    )

    chunk_topic = (
        new_topic
        + "/chunk/+"
    )

    meta_topic = (
        new_topic
        + "/meta"
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

    for index in range(
        chunk_count
    ):

        expected_chunks.add(
            f"{new_topic}/chunk/"
            f"{index:06d}"
        )

    start_time = time.time()

    while True:

        with received_lock:

            current = set(
                received_messages.keys()
            )

        received_chunks = (
            current
            & expected_chunks
        )

        if (
            len(received_chunks)
            == chunk_count
            and meta_topic in current
        ):

            print(
                "[VERIFY] "
                "新MQTTデータ確認OK"
            )

            break

        if (
            time.time() - start_time
            >= RETAIN_RECEIVE_TIMEOUT
        ):

            raise TimeoutError(
                "新MQTTデータ確認タイムアウト"
            )

        time.sleep(0.2)

    try:

        mqtt_client.unsubscribe(
            chunk_topic
        )

        mqtt_client.unsubscribe(
            meta_topic
        )

    except Exception:

        pass

    return True


# ============================================================
# 古いMQTT Retain削除
# ============================================================

def delete_old_mqtt(
    file_info
):

    old_topic = file_info["mqtt_topic"]

    chunk_count = int(
        file_info["chunk_count"]
    )

    print("")
    print(
        "[DELETE OLD MQTT]"
    )

    print(
        "topic:",
        old_topic
    )

    # --------------------------------------------------------
    # meta削除
    # --------------------------------------------------------

    publish_with_retry(
        topic=(
            old_topic
            + "/meta"
        ),
        payload=b"",
        retain=True,
        qos=0
    )

    # --------------------------------------------------------
    # chunk削除
    # --------------------------------------------------------

    for index in range(
        chunk_count
    ):

        old_chunk_topic = (
            f"{old_topic}/chunk/"
            f"{index:06d}"
        )

        print(
            f"[DELETE OLD MQTT] "
            f"{index + 1}/{chunk_count}"
        )

        publish_with_retry(
            topic=old_chunk_topic,
            payload=b"",
            retain=True,
            qos=0
        )

    print(
        "[DELETE OLD MQTT] "
        "削除完了"
    )


# ============================================================
# 古いDBレコード削除
# ============================================================

def delete_old_db_record(
    old_id
):

    sql = """
        DELETE FROM public.shared_files
        WHERE id = %s
    """

    with psycopg.connect(
        DATABASE_URL
    ) as conn:

        with conn.cursor() as cur:

            cur.execute(
                sql,
                (old_id,)
            )

            deleted = cur.rowcount

        conn.commit()

    print(
        "[DB] 古いレコード削除:",
        old_id,
        "rows=",
        deleted
    )


# ============================================================
# 1ファイル再作成
# ============================================================

def rebuild_file(
    file_info
):

    old_id = str(
        file_info["id"]
    )

    # --------------------------------------------------------
    # 新しいID
    # --------------------------------------------------------

    new_id = create_new_file_id()

    new_topic = create_new_topic(
        new_id
    )

    print("")
    print(
        "########################################"
    )

    print(
        "FILE:",
        file_info["file_name"]
    )

    print(
        "OLD ID:",
        old_id
    )

    print(
        "NEW ID:",
        new_id
    )

    print(
        "########################################"
    )

    # ========================================================
    # 1. 元データ読み込み
    # ========================================================

    old_meta, chunks = (
        read_old_mqtt_data(
            file_info
        )
    )

    # ========================================================
    # 2. 新IDでMQTT作成
    # ========================================================

    publish_new_file(
        file_info,
        old_meta,
        chunks,
        new_id,
        new_topic
    )

    # ========================================================
    # 3. 新MQTT確認
    # ========================================================

    verify_new_mqtt_data(
        file_info,
        new_id,
        new_topic
    )

    # ========================================================
    # 4. 新DBレコード作成
    # ========================================================

    try:

        insert_new_db_record(
            file_info,
            new_id,
            new_topic
        )

    except Exception as e:

        print(
            "[ERROR] "
            "新DBレコード作成失敗"
        )

        print(e)

        # ----------------------------------------------------
        # 新MQTTを削除
        # ----------------------------------------------------

        print(
            "[ROLLBACK] "
            "新MQTTを削除します"
        )

        temp_file = dict(
            file_info
        )

        temp_file["mqtt_topic"] = (
            new_topic
        )

        try:

            delete_old_mqtt(
                temp_file
            )

        except Exception as cleanup_error:

            print(
                "[ROLLBACK] "
                "新MQTT削除失敗:",
                cleanup_error
            )

        raise

    # ========================================================
    # 5. ここまで成功したので
    #    初めて旧データを削除
    # ========================================================

    print("")
    print(
        "[OLD DATA] "
        "新データ作成確認済み"
    )

    print(
        "[OLD DATA] "
        "これから旧データを削除します"
    )

    # --------------------------------------------------------
    # 旧MQTT削除
    # --------------------------------------------------------

    delete_old_mqtt(
        file_info
    )

    # --------------------------------------------------------
    # 旧DB削除
    # --------------------------------------------------------

    delete_old_db_record(
        old_id
    )

    print("")
    print(
        "========================================"
    )

    print(
        "[SUCCESS] "
        "ファイル再作成完了"
    )

    print(
        "file:",
        file_info["file_name"]
    )

    print(
        "old ID:",
        old_id
    )

    print(
        "new ID:",
        new_id
    )

    print(
        "========================================"
    )


# ============================================================
# メイン
# ============================================================

def main():

    print("")
    print(
        "=" * 70
    )

    print(
        "ChanPro Shared Files"
    )

    print(
        "MQTT ID REBUILD"
    )

    print(
        "=" * 70
    )

    print(
        "Publish timeout:",
        MQTT_PUBLISH_TIMEOUT,
        "seconds"
    )

    print(
        "Receive timeout:",
        RETAIN_RECEIVE_TIMEOUT,
        "seconds"
    )

    print(
        "Retry:",
        PUBLISH_RETRY_COUNT
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # DB
    # --------------------------------------------------------

    files = get_shared_files()

    if not files:

        print(
            "対象ファイルがありません"
        )

        return

    print(
        f"{len(files)}件のファイルを"
        "処理します"
    )

    # --------------------------------------------------------
    # MQTT
    # --------------------------------------------------------

    connect_mqtt()

    success = 0
    failed = 0

    errors = []

    # --------------------------------------------------------
    # 全ファイル
    # --------------------------------------------------------

    for index, file_info in enumerate(
        files,
        start=1
    ):

        print("")
        print(
            f"========== "
            f"{index}/{len(files)} "
            f"=========="
        )

        try:

            rebuild_file(
                file_info
            )

            success += 1

        except Exception as e:

            failed += 1

            message = (
                f"{file_info['file_name']}: "
                f"{e}"
            )

            errors.append(
                message
            )

            print(
                "[FAILED]",
                message
            )

            # ------------------------------------------------
            # 失敗しても次のファイルへ
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
    print(
        "=" * 70
    )

    print(
        "処理完了"
    )

    print(
        f"成功: {success}"
    )

    print(
        f"失敗: {failed}"
    )

    print(
        f"合計: {len(files)}"
    )

    if errors:

        print("")
        print(
            "失敗一覧:"
        )

        for error in errors:

            print(
                " -",
                error
            )

    print(
        "=" * 70
    )

    if failed > 0:

        raise RuntimeError(
            f"{failed}件の"
            "ファイルで失敗しました"
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        print(
            "中断しました"
        )

        sys.exit(130)

    except Exception as e:

        print(
            "ERROR:",
            e
        )

        sys.exit(1)
