import json
import time
import sys
import paho.mqtt.client as mqtt

MQTT_BROKER = "e2207633d3d84d30a58f34fb439d5580.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "Maurrici"
MQTT_PASSWORD = "@123456mM"
MQTT_TOPIC_READ = "temperature"
MQTT_TOPIC_WRITE = "alerts"

INTERVAL_MEDIAN_SEC = 120
DIFF_TEMP = 5.0
HIGH_TEMPERATURE = 200.0

temperature_buffer = [] 
previous_average = None

MAX_RETRIES = 5
RECONNECT_DELAY_S = 2

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado com sucesso ao broker HiveMQ (ID: {client._client_id.decode()}).")
        client.subscribe(MQTT_TOPIC_READ, qos=1)
        print(f"Subscrito ao tópico: {MQTT_TOPIC_READ}")
    else:
        print(f"Falha na conexão. Código de erro: {rc}. Tentando reconectar...")

def on_message(client, userdata, msg):
    global temperature_buffer
    
    try:
        payload = json.loads(msg.payload.decode())
        current_temp = payload.get("temperature")
        timestamp = payload.get("timestamp")
        sensor_id = payload.get("sensor_id")
        
        if current_temp is None or timestamp is None:
            print("AVISO: Payload incompleto recebido.")
            return

        temperature_buffer.append({"temp": current_temp, "time": timestamp})
        
        print(f"\n[RECEBIDO] SENSOR {sensor_id}: {current_temp}°C @ {time.strftime('%H:%M:%S', time.localtime(timestamp))}")

        calculate_and_publish_average(client, timestamp)

    except json.JSONDecodeError:
        print(f"ERRO: Não foi possível decodificar o payload JSON: {msg.payload.decode()}")
    except Exception as e:
        print(f"ERRO inesperado ao processar mensagem: {e}")

def connect_to_mqtt():
    client = mqtt.Client(
        client_id=f"CAT_Service_001",
        protocol=mqtt.MQTTv311,
        transport="tcp"
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.tls_set()
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Tentativa de conexão {attempt + 1}/{MAX_RETRIES}...")
            client.connect(MQTT_BROKER, MQTT_PORT)
            return client
        except Exception as e:
            print(f"Erro ao conectar: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RECONNECT_DELAY_S * (2 ** attempt))
    
    print("Não foi possível conectar após várias tentativas. Encerrando.")
    sys.exit(1)

def cleanup_buffer(current_time):
    global temperature_buffer
    
    cutoff_time = current_time - INTERVAL_MEDIAN_SEC
    temperature_buffer = [item for item in temperature_buffer if item["time"] >= cutoff_time]
    
    print(f"-> Buffer atual: {len(temperature_buffer)} leituras nos últimos {INTERVAL_MEDIAN_SEC}s.")

def calculate_and_publish_average(client, current_time):
    global temperature_buffer, previous_average
    
    cleanup_buffer(current_time)
    
    if not temperature_buffer:
        return

    total_temp = sum(item["temp"] for item in temperature_buffer)
    current_average = round(total_temp / len(temperature_buffer), 2)
    
    print(f"-> Média dos últimos {INTERVAL_MEDIAN_SEC}s: {current_average}°C")
    
    alerts_to_publish = []

    if previous_average is not None:
        diff = abs(current_average - previous_average)
        if diff > DIFF_TEMP:
            alert_payload = {
                "alert_type": 1,
                "value": diff,
                "timestamp": current_time
            }
            alerts_to_publish.append(alert_payload)

    if current_average > HIGH_TEMPERATURE:
        alert_payload = {
            "alert_type": 2,
            "value": current_average,
            "timestamp": current_time,
        }
        alerts_to_publish.append(alert_payload)

    previous_average = current_average
    
    for alert in alerts_to_publish:
        alert_str = json.dumps(alert)
        client.publish(MQTT_TOPIC_WRITE, alert_str, qos=1)
        print(f"-> ALERTA PUBLICADO no tópico {MQTT_TOPIC_WRITE}: {alert['alert_type']}")


def main():    
    client = connect_to_mqtt()
    
    print("\n--- SERVIÇO COMPUTE AVERAGE TEMPERATURE (CAT) INICIADO ---")
    
    try:
        client.loop_forever() 

    except KeyboardInterrupt:
        print("\nServiço CAT encerrado pelo usuário.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
    finally:
        client.disconnect()
        print("Conexão MQTT desconectada.")

if __name__ == "__main__":
    if MQTT_BROKER.startswith("SEU_BROKER"):
        print("ERRO: Por favor, atualize as variáveis MQTT_BROKER, MQTT_USER e MQTT_PASSWORD no código.")
        sys.exit(1)

    main()