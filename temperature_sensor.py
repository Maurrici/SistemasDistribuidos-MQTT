import paho.mqtt.client as mqtt
import time
import json
import random
import sys

MQTT_BROKER = "e2207633d3d84d30a58f34fb439d5580.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "Maurrici"
MQTT_PASSWORD = "@123456mM"
MQTT_TOPIC = "temperature"
SENSOR_ID = "temperature_001"
INTERVAL_READ_SEC = 60

MAX_RETRIES = 5
RECONNECT_DELAY_S = 2

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Conectado com sucesso ao broker HiveMQ (ID: {client._client_id.decode()}).")
    else:
        print(f"Falha na conexão. Código de erro: {rc}. Tentando reconectar...")

def on_publish(client, userdata, mid):
    pass

def connect_to_mqtt():
    client = mqtt.Client(
        client_id=f"Sensor_{SENSOR_ID}",
        protocol=mqtt.MQTTv311,
        transport="tcp"
    )
    client.on_connect = on_connect
    client.on_publish = on_publish

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

def simular_temperatura(etapa):
    if etapa == 0:
        return 80.0
    elif 1 <= etapa <= 3:
        temp = 80.0 + (etapa * 5.0)
    elif etapa == 4:
        temp = 110.0
    elif etapa == 5:
        temp = 120.0
    elif 6 <= etapa <= 10:
        temp = 120.0 + ((etapa - 5) * 20.0)    
    else:
        temp = 220.0 + random.uniform(-2.0, 5.0)
    
    return round(temp + random.uniform(-0.5, 0.5), 2)

def main():
    etapa_simulacao = 0
    
    client = connect_to_mqtt()
    client.loop_start() 

    print("\n--- SIMULADOR DE SENSOR INICIADO ---")
    print(f"Publicando em: {MQTT_TOPIC}")

    try:
        while True:
            current_temp = simular_temperatura(etapa_simulacao)
            timestamp = int(time.time())
            
            payload = {
                "sensor_id": SENSOR_ID,
                "temperature": current_temp,
                "timestamp": timestamp,
            }
            
            payload_str = json.dumps(payload)
            
            print(f"[{etapa_simulacao:02d}] Publicando: {payload_str}")
            
            client.publish(MQTT_TOPIC, payload_str, qos=1) 
            
            etapa_simulacao += 1
            
            time.sleep(INTERVAL_READ_SEC)

    except KeyboardInterrupt:
        print("\nSimulador encerrado pelo usuário.")
    except Exception as e:
        print(f"Ocorreu um erro inesperado no loop principal: {e}")
    finally:
        client.loop_stop()
        client.disconnect()
        print("Conexão MQTT desconectada.")

if __name__ == "__main__":
    if MQTT_BROKER.startswith("SEU_BROKER"):
        print("ERRO: Por favor, atualize as variáveis MQTT_BROKER, MQTT_USER e MQTT_PASSWORD no código.")
        sys.exit(1)
        
    main()