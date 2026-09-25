import os
import time
import ssl
import json
import threading

import psycopg
import paho.mqtt.client as mqtt


# ============================================================
# 設定
# ============================================================

MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))

MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

MQTT_BASE = "chanpro-share-v2"
MQTT_FILE_BASE = f"{MQTT_BASE}/file"

DATABASE_URL = os.environ["DATABASE_URL"]

MQTT_TIMEOUT = 30
PUBLISH_TIMEOUT = 300

# 1回の実行で処理する最大ファイル数
MAX_FILES = 100


# ============================================================
# MQTT
# ============================================================

client = None

connected_event = threading.Event()
received_event = threading.Event()

received_messages = {}

lock = threading.Lock()


def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] connected rc={rc}")

    if rc == 0:
        connected_event.set()


def on_disconnect(client, userdata, rc):
    print(f"[MQTT] disconnected rc={rc}")


def on_message(client, userdata, msg):
    topic = msg.topic

    with lock:
        received_messages[topic] = bytes(msg.payload)

    print(
        f"[MQTT] received: "
        f"{topic} "
        f"({len(msg.payload)} bytes)"
    )

    received_event.set()


def create_mqtt_client():

    global client

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION1,
        client_id=f"chanpro-refresh-{os.getpid()}",
        clean_session=True,
    )

    if MQTT_USERNAME:
        client.username_pw_set(
            MQTT_USERNAME,
            MQTT_PASSWORD
        )

    client.tls_set(
        tls_version=ssl.PROTOCOL_TLS_CLIENT
    )

    client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    print(
        f"[MQTT] connecting "
        f"{MQTT_HOST}:{MQTT_PORT}"
    )

    client.connect(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60
    )

    client.loop_start()

    if not connected_event.wait(MQTT_TIMEOUT):
        raise TimeoutError(
            "MQTT connection timeout"
        )


# ============================================================
# Supabase / PostgreSQL
# ============================================================

def load_files():

    print("[DB] loading shared_files")

    with psycopg.connect(DATABASE_URL) as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
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
                """,
                (MAX_FILES,)
            )

            rows = cur.fetchall()

    files = []

    for row in rows:

        files.append(
            {
                "id": str(row[0]),
                "user_id": str(row[1]),
                "file_name": row[2],
                "file_size": int(row[3]),
                "mime_type": row[4],
                "chunk_count": int(row[5]),
                "chunk_size": int(row[6]),
                "mqtt_topic": row[7],
                "created_at": (
                    row[8].isoformat()
                    if row[8]
                    else None
                ),
            }
        )

    print(
        f"[DB] {len(files)} files found"
    )

    return files


# ============================================================
# MQTT Retain取得
# ============================================================

def request_retained_topic(topic):

    global received_messages

    with lock:
        received_messages = {}

    received_event.clear()

    print(
        f"[MQTT] subscribe: {topic}"
    )

    result, mid = client.subscribe(
        topic,
        qos=0
    )

    if result != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(
            f"subscribe failed: {result}"
        )

    # Retainメッセージが届くまで待つ
    deadline = time.time() + MQTT_TIMEOUT

    while time.time() < deadline:

        with lock:
            if topic in received_messages:
                payload = received_messages[topic]

                # 受信後にunsubscribe
                client.unsubscribe(topic)

                return payload

        time.sleep(0.1)

    client.unsubscribe(topic)

    raise TimeoutError(
        f"Retained message timeout: {topic}"
    )


# ============================================================
# Retain再保存
# ============================================================

def republish_retain(topic, payload):

    print(
        f"[MQTT] republish: "
        f"{topic} "
        f"{len(payload)} bytes"
    )

    info = client.publish(
        topic,
        payload,
        qos=0,
        retain=True
    )

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(
            f"publish failed: "
            f"{topic} rc={info.rc}"
        )

    if not info.wait_for_publish(
        timeout=PUBLISH_TIMEOUT
    ):
        raise TimeoutError(
            f"publish timeout: {topic}"
        )


# ============================================================
# ファイル1個を再保存
# ============================================================

def refresh_file(file):

    mqtt_topic = file["mqtt_topic"]
    chunk_count = file["chunk_count"]

    print("")
    print("=" * 70)
    print(
        f"FILE: {file['file_name']}"
    )
    print(
        f"SIZE: {file['file_size']} bytes"
    )
    print(
        f"CHUNKS: {chunk_count}"
    )
    print(
        f"TOPIC: {mqtt_topic}"
    )
    print("=" * 70)

    # --------------------------------------------------------
    # metadata
    # --------------------------------------------------------

    meta_topic = f"{mqtt_topic}/meta"

    try:

        meta = request_retained_topic(
            meta_topic
        )

        if meta is None:
            raise RuntimeError(
                "metadata retained message not found"
            )

        republish_retain(
            meta_topic,
            meta
        )

        print(
            "[OK] metadata refreshed"
        )

    except Exception as e:

        print(
            f"[WARN] metadata refresh failed: {e}"
        )

    # --------------------------------------------------------
    # chunks
    # --------------------------------------------------------

    success = 0
    failed = 0

    for index in range(chunk_count):

        chunk_topic = (
            f"{mqtt_topic}/chunk/"
            f"{index:06d}"
        )

        try:

            payload = request_retained_topic(
                chunk_topic
            )

            if payload is None:
                raise RuntimeError(
                    "chunk retained message not found"
                )

            republish_retain(
                chunk_topic,
                payload
            )

            success += 1

        except Exception as e:

            failed += 1

            print(
                f"[ERROR] "
                f"chunk {index}: {e}"
            )

    print(
        f"[RESULT] "
        f"{file['file_name']} "
        f"success={success} "
        f"failed={failed}"
    )

    return failed == 0


# ============================================================
# main
# ============================================================

def main():

    print("")
    print("=" * 70)
    print("ChanPro Shared Files MQTT Retain Refresh")
    print("=" * 70)

    files = load_files()

    if not files:
        print(
            "[INFO] no shared files"
        )
        return

    create_mqtt_client()

    total = len(files)
    success = 0
    failed = 0

    try:

        for index, file in enumerate(
            files,
            start=1
        ):

            print("")
            print(
                f"[{index}/{total}] "
                f"{file['file_name']}"
            )

            try:

                if refresh_file(file):
                    success += 1
                else:
                    failed += 1

            except Exception as e:

                failed += 1

                print(
                    f"[ERROR] "
                    f"{file['file_name']}: {e}"
                )

    finally:

        if client:
            client.loop_stop()
            client.disconnect()

    print("")
    print("=" * 70)
    print("REFRESH COMPLETE")
    print(
        f"files={total} "
        f"success={success} "
        f"failed={failed}"
    )
    print("=" * 70)

    if failed:
        raise RuntimeError(
            f"{failed} file(s) failed"
        )


if __name__ == "__main__":
    main()
