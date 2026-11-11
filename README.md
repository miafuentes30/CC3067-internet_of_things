# Estación Meteorológica IoT con Apache Kafka

## Archivos

| Archivo | Descripción |
|---|---|
| `config.py` | Carga el servidor, topic y autenticación desde variables de entorno. |
| `codec.py` | Codifica y decodifica el formato compacto de 3 bytes. |
| `producer.py` | Kafka Producer: genera lecturas y las envía en JSON o formato compacto. |
| `consumer.py` | Kafka Consumer: reconoce ambos formatos y grafica la telemetría en vivo. |

## Datos simulados

| Sensor | Rango | Tipo | Generación |
|---|---|---|---|
| Temperatura | [0, 110.00] °C | float, 2 decimales | Normal, media 55, desviación estándar 15 |
| Humedad relativa | [0, 100] % | entero | Normal, media 55, desviación estándar 15 |
| Dirección del viento | N, NO, O, SO, S, SE, E, NE | categórico | Uniforme |

Los valores que caen fuera del rango se recortan al límite más cercano. Cada
lectura se envía como JSON, con la key `sensor1`:

```json
{"temperatura": 56.32, "humedad": 51, "direccion_viento": "SO"}
```

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Copie `.env.example` como `.env` y complete los datos de conexión y las
credenciales de Kafka. El archivo `.env` está excluido de Git.

## Ejecución

En una terminal, el consumidor (abre la ventana con las gráficas):

```bash
python consumer.py
```

En otra terminal, el productor:

```bash
python producer.py            # una lectura cada 15-30 segundos
python producer.py --prueba   # una lectura cada 2 segundos, para probar
python producer.py --compacto # payload binario de exactamente 3 bytes
python producer.py --compacto --prueba  # modo compacto cada 2 segundos
```

El consumidor detecta automáticamente los mensajes JSON y compactos, por lo que
se ejecuta de la misma forma para ambas modalidades. Ambos se detienen con
`Ctrl+C`. El consumidor lee el topic desde el inicio, así
que grafica tanto la telemetría pasada como la que va entrando, y guarda la
gráfica en `grafica_telemetria.png` cada vez que se actualiza.
