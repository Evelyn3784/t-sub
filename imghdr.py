# imghdr.py
# Небольшая реализация стандартного модуля imghdr (совместимая с Telethon)
# Поддерживает: jpeg, png, gif, tiff, bmp, webp, ico, svg (по сигнатуре), ppm/pgm/pbm
# Эта версия читает заголовок и определяет тип файла по магическим числам.

def _get_bytes(header, n):
    if header is None:
        return b''
    return header[:n]

def what(file, h=None):
    """
    Определить тип изображения.
    file -- либо путь к файлу (str/bytes/Path), либо объект с read()
    h -- необязательные первые байты (bytes)
    Возвращает строку типа: 'jpeg', 'png', 'gif', ... либо None.
    """
    header = h
    if header is None:
        # если передан путь или файловый объект - прочитать заголовок
        try:
            if hasattr(file, "read"):
                pos = None
                try:
                    pos = file.tell()
                except Exception:
                    pos = None
                header = file.read(64)
                if pos is not None:
                    try:
                        file.seek(pos)
                    except Exception:
                        pass
            else:
                with open(file, "rb") as f:
                    header = f.read(64)
        except Exception:
            header = b""

    # ensure bytes
    if header is None:
        header = b""
    if not isinstance(header, (bytes, bytearray)):
        try:
            header = bytes(header)
        except Exception:
            header = b""

    h = header

    # JPEG (starts with 0xFFD8)
    if len(h) >= 2 and h[0:2] == b'\xff\xd8':
        return "jpeg"

    # PNG (89 50 4E 47 0D 0A 1A 0A)
    if h.startswith(b'\x89PNG\r\n\x1a\n'):
        return "png"

    # GIF (GIF87a or GIF89a)
    if h.startswith(b'GIF87a') or h.startswith(b'GIF89a'):
        return "gif"

    # TIFF (II* or MM*\x00)
    if h.startswith(b'II*\x00') or h.startswith(b'MM\x00*'):
        return "tiff"

    # BMP (BM)
    if h.startswith(b'BM'):
        return "bmp"

    # WEBP (RIFF....WEBP)
    if len(h) >= 12 and h[0:4] == b'RIFF' and h[8:12] == b'WEBP':
        return "webp"

    # ICO (00 00 01 00)
    if h.startswith(b'\x00\x00\x01\x00'):
        return "ico"

    # SVG - xml header <?xml ... and contains <svg
    # SVG detection is heuristic: check for "<svg" in first 256 bytes (text)
    try:
        txt = h.decode('utf-8', errors='ignore').lower()
        if '<svg' in txt:
            return "svg"
    except Exception:
        pass

    # PBM/PGM/PPM (ASCII headers P1-P6)
    if len(h) >= 2 and h[0:2] in (b'P1', b'P2', b'P3', b'P4', b'P5', b'P6'):
        # P1/P4 -> pbm, P2/P5 -> pgm, P3/P6 -> ppm (P1-P6 mapping)
        tag = h[1:2]
        if tag in (b'1', b'4'):
            return "pbm"
        if tag in (b'2', b'5'):
            return "pgm"
        if tag in (b'3', b'6'):
            return "ppm"

    # JPEG 2000 (file header starts with 0x0000000cjP 0j?) - look for 'jP  ' signature
    if len(h) >= 12 and h[4:12] == b'ftypjp2':
        return "jp2"

    # fallback: None
    return None
