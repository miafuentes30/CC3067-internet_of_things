"""
Configuracion compartida por el Producer y el Consumer.

Laboratorio: IoT - Estacion Meteorologica (CC3067 Redes, UVG)
"""

import os

from dotenv import load_dotenv

load_dotenv()


def _variable_requerida(nombre: str) -> str:
    valor = os.getenv(nombre)
    if valor is None or not valor.strip():
        raise RuntimeError(f"Falta definir la variable de entorno {nombre}")
    return valor

# --- Servidor Kafka (Edge Server) -------------------------------------------
BOOTSTRAP_SERVERS = _variable_requerida("KAFKA_BOOTSTRAP_SERVERS")

# Topic unico de la pareja (numero de carne)
TOPIC = _variable_requerida("KAFKA_TOPIC")

# Key con la que se publican los mensajes (identifica al nodo de sensores)
SENSOR_KEY = _variable_requerida("KAFKA_SENSOR_KEY")

# --- Autenticacion ----------------------------------------------------------
SECURITY_PROTOCOL = _variable_requerida("KAFKA_SECURITY_PROTOCOL")
SASL_MECHANISM = _variable_requerida("KAFKA_SASL_MECHANISM")
SASL_USERNAME = _variable_requerida("KAFKA_SASL_USERNAME")
SASL_PASSWORD = _variable_requerida("KAFKA_SASL_PASSWORD")

# Parametros comunes para KafkaProducer y KafkaConsumer
KAFKA_AUTH = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "security_protocol": SECURITY_PROTOCOL,
    "sasl_mechanism": SASL_MECHANISM,
    "sasl_plain_username": SASL_USERNAME,
    "sasl_plain_password": SASL_PASSWORD,
}

# --- Sensores ---------------------------------------------------------------
# Temperatura: float de dos decimales en [0, 110.00] grados C
TEMP_MIN, TEMP_MAX = 0.0, 110.0
# Humedad relativa: entero en [0, 100] %
HUM_MIN, HUM_MAX = 0, 100
# Direccion del viento
DIRECCIONES_VIENTO = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]

# Distribucion normal (gaussiana) centrada en 55 para temperatura y humedad.
# Con una desviacion estandar de 15 (varianza = 225), ~99.7% de las muestras
# caen en [10, 100], por lo que casi nunca hay que recortar al rango valido.
MEDIA = 55.0
DESVIACION_ESTANDAR = 15.0

# --- Envio ------------------------------------------------------------------
# Se envia una lectura cada 15 a 30 segundos
INTERVALO_MIN_S = 15
INTERVALO_MAX_S = 30
