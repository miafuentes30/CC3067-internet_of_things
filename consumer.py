"""
Consumer: se suscribe al topic de la estacion meteorologica, consume la
telemetria (pasada y entrante) y la grafica en vivo, actualizando el grafico
cada vez que llega un dato nuevo.

Uso:
    python consumer.py

Se detiene con Ctrl+C o cerrando la ventana del grafico. Cada actualizacion
tambien se guarda en 'grafica_telemetria.png'.
"""

import json
from datetime import datetime

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

import config
from codec import decode

ARCHIVO_GRAFICA = "grafica_telemetria.png"

# Telemetria acumulada
all_time = []
all_temp = []
all_hume = []
all_wind = []


def crear_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        config.TOPIC,
        **config.KAFKA_AUTH,
        # Sin consumer group: cada ejecucion lee el topic desde el inicio, de
        # modo que se grafica tanto la telemetria pasada como la entrante.
        group_id=None,
        auto_offset_reset="earliest",
    )


def procesar_mensaje(mensaje):
    """Decodifica JSON o el formato compacto. Devuelve None si no es valido."""
    try:
        if len(mensaje.value) == 3:
            payload = decode(mensaje.value)
        else:
            payload = json.loads(mensaje.value.decode("utf-8"))
        temperatura = float(payload["temperatura"])
        humedad = int(payload["humedad"])
        direccion = payload["direccion_viento"]
    except (ValueError, KeyError, TypeError, AttributeError, UnicodeDecodeError):
        return None
    if direccion not in config.DIRECCIONES_VIENTO:
        return None
    return {
        "temperatura": temperatura,
        "humedad": humedad,
        "direccion_viento": direccion,
    }


def crear_figura():
    fig, (ax_temp, ax_hume, ax_wind) = plt.subplots(3, 1, figsize=(12, 9), sharex=True)
    fig.canvas.manager.set_window_title(f"Estacion Meteorologica - topic {config.TOPIC}")
    return fig, (ax_temp, ax_hume, ax_wind)


def plot_all_data(fig, ejes) -> None:
    """Redibuja las tres graficas con toda la telemetria recibida."""
    ax_temp, ax_hume, ax_wind = ejes
    for ax in ejes:
        ax.clear()
        ax.grid(True, alpha=0.3)

    fig.suptitle(
        f"Estación Meteorológica IoT — topic {config.TOPIC} — "
        f"{len(all_time)} lecturas (última: {all_time[-1]:%H:%M:%S})",
        fontsize=13,
        fontweight="bold",
    )

    ax_temp.plot(all_time, all_temp, color="tab:red", marker="o", markersize=4, linewidth=1.5)
    ax_temp.set_ylabel("Temperatura (°C)")
    ax_temp.set_ylim(config.TEMP_MIN, config.TEMP_MAX)
    ax_temp.set_title(f"Temperatura — actual: {all_temp[-1]:.2f} °C", loc="left", fontsize=10)

    ax_hume.plot(all_time, all_hume, color="tab:blue", marker="o", markersize=4, linewidth=1.5)
    ax_hume.set_ylabel("Humedad relativa (%)")
    ax_hume.set_ylim(config.HUM_MIN, config.HUM_MAX)
    ax_hume.set_title(f"Humedad relativa — actual: {all_hume[-1]} %", loc="left", fontsize=10)

    indices = [config.DIRECCIONES_VIENTO.index(d) for d in all_wind]
    ax_wind.step(all_time, indices, where="post", color="tab:green", linewidth=1, alpha=0.5)
    ax_wind.scatter(all_time, indices, color="tab:green", s=25, zorder=3)
    ax_wind.set_yticks(range(len(config.DIRECCIONES_VIENTO)))
    ax_wind.set_yticklabels(config.DIRECCIONES_VIENTO)
    ax_wind.set_ylim(-0.5, len(config.DIRECCIONES_VIENTO) - 0.5)
    ax_wind.set_ylabel("Dirección del viento")
    ax_wind.set_title(f"Dirección del viento — actual: {all_wind[-1]}", loc="left", fontsize=10)
    ax_wind.set_xlabel("Hora de la lectura")
    ax_wind.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.canvas.draw_idle()
    fig.savefig(ARCHIVO_GRAFICA, dpi=200)


def main() -> None:
    consumer = crear_consumer()
    print(f"Conectado a {config.BOOTSTRAP_SERVERS}. Escuchando el topic '{config.TOPIC}'.")
    print("Ctrl+C o cerrar la ventana para detener.\n")

    plt.ion()
    fig, ejes = crear_figura()
    plt.show(block=False)

    try:
        while plt.fignum_exists(fig.number):
            # poll() con timeout corto para que la ventana no se congele
            lotes = consumer.poll(timeout_ms=500)
            hay_nuevos = False

            for mensajes in lotes.values():
                for mensaje in mensajes:
                    payload = procesar_mensaje(mensaje)
                    if payload is None:
                        print(f"Mensaje ignorado (formato no valido): {mensaje.value!r}")
                        continue

                    # timestamp que Kafka le asigno al mensaje (en milisegundos)
                    hora = datetime.fromtimestamp(mensaje.timestamp / 1000)
                    all_time.append(hora)
                    all_temp.append(payload["temperatura"])
                    all_hume.append(payload["humedad"])
                    all_wind.append(payload["direccion_viento"])
                    hay_nuevos = True
                    print(f"[{hora:%H:%M:%S}] offset {mensaje.offset} -> {payload}")

            if hay_nuevos:
                plot_all_data(fig, ejes)

            plt.pause(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\nConsumer detenido. Lecturas graficadas: {len(all_time)}")
        if all_time:
            print(f"Grafica guardada en {ARCHIVO_GRAFICA}")


if __name__ == "__main__":
    main()
