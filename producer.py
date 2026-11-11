"""
Producer: simula el nodo de sensores de una estacion meteorologica (el
"dispositivo IoT") y publica su telemetria en formato JSON a un topic de Kafka.

Uso:
    python producer.py            # envia una lectura cada 15-30 segundos
    python producer.py --prueba   # envia cada 2 segundos (solo para probar)
    python producer.py --compacto # envia cada lectura en exactamente 3 bytes

Se detiene con Ctrl+C.
"""

import argparse
import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

import config
from codec import encode


def generar_temperatura() -> float:
    """Temperatura en grados C: normal(55, 15) recortada a [0, 110], 2 decimales."""
    valor = random.gauss(config.MEDIA, config.DESVIACION_ESTANDAR)
    valor = max(config.TEMP_MIN, min(config.TEMP_MAX, valor))
    return round(valor, 2)


def generar_humedad() -> int:
    """Humedad relativa en %: normal(55, 15) recortada a [0, 100], entero."""
    valor = round(random.gauss(config.MEDIA, config.DESVIACION_ESTANDAR))
    return max(config.HUM_MIN, min(config.HUM_MAX, valor))


def generar_direccion_viento() -> str:
    """Direccion del viento: distribucion uniforme sobre las 8 direcciones."""
    return random.choice(config.DIRECCIONES_VIENTO)


def generar_data() -> dict:
    """Una lectura completa del nodo de sensores."""
    return {
        "temperatura": generar_temperatura(),
        "humedad": generar_humedad(),
        "direccion_viento": generar_direccion_viento(),
    }


def crear_producer(compacto: bool = False) -> KafkaProducer:
    return KafkaProducer(
        **config.KAFKA_AUTH,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=encode if compacto else lambda v: json.dumps(v).encode("utf-8"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer de la estacion meteorologica")
    parser.add_argument(
        "--prueba",
        action="store_true",
        help="envia una lectura cada 2 segundos en lugar de cada 15-30",
    )
    parser.add_argument(
        "--compacto",
        action="store_true",
        help="envia cada lectura como un payload binario de 3 bytes",
    )
    args = parser.parse_args()

    producer = crear_producer(args.compacto)
    print(f"Conectado a {config.BOOTSTRAP_SERVERS}. Publicando en el topic '{config.TOPIC}'.")
    print(f"Formato: {'compacto (3 bytes)' if args.compacto else 'JSON'}.")
    print("Ctrl+C para detener.\n")

    enviados = 0
    try:
        while True:
            data = generar_data()
            futuro = producer.send(config.TOPIC, key=config.SENSOR_KEY, value=data)
            metadata = futuro.get(timeout=30)  # espera la confirmacion del broker
            enviados += 1

            hora = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{hora}] #{enviados} enviado -> {json.dumps(data)} "
                f"(particion {metadata.partition}, offset {metadata.offset})"
            )

            if args.prueba:
                espera = 2
            else:
                espera = random.uniform(config.INTERVALO_MIN_S, config.INTERVALO_MAX_S)
            time.sleep(espera)
    except KeyboardInterrupt:
        print(f"\nInterrumpido. Lecturas enviadas: {enviados}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
