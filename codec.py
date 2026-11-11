"""Codificacion compacta de la telemetria en un payload de 24 bits."""

import config


def encode(data: dict) -> bytes:
    """Codifica temperatura (14 bits), humedad (7) y viento (3) en 3 bytes."""
    try:
        temperatura = float(data["temperatura"])
        humedad = int(data["humedad"])
        direccion = data["direccion_viento"]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Lectura meteorologica no valida") from exc

    if not config.TEMP_MIN <= temperatura <= config.TEMP_MAX:
        raise ValueError("La temperatura debe estar entre 0 y 110.00")
    temperatura_entera = round(temperatura * 100)
    if isinstance(data["humedad"], float) and not data["humedad"].is_integer():
        raise ValueError("La humedad debe ser un entero entre 0 y 100")
    if not 0 <= humedad <= 100:
        raise ValueError("La humedad debe estar entre 0 y 100")
    if direccion not in config.DIRECCIONES_VIENTO:
        raise ValueError("Direccion del viento no valida")

    direccion_entera = config.DIRECCIONES_VIENTO.index(direccion)
    valor = (temperatura_entera << 10) | (humedad << 3) | direccion_entera
    return valor.to_bytes(3, byteorder="big")


def decode(payload: bytes) -> dict:
    """Decodifica un payload compacto de exactamente 3 bytes."""
    if not isinstance(payload, bytes) or len(payload) != 3:
        raise ValueError("El payload compacto debe contener exactamente 3 bytes")

    valor = int.from_bytes(payload, byteorder="big")
    temperatura_entera = (valor >> 10) & 0x3FFF
    humedad = (valor >> 3) & 0x7F
    direccion_entera = valor & 0x07

    if temperatura_entera > 11000 or humedad > 100:
        raise ValueError("El payload contiene valores fuera de rango")

    return {
        "temperatura": temperatura_entera / 100,
        "humedad": humedad,
        "direccion_viento": config.DIRECCIONES_VIENTO[direccion_entera],
    }
