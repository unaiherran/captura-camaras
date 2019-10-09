import requests
import settings
from PIL import Image
from io import BytesIO

from datetime import datetime

import logging


def crawl(lista_camaras = settings.lista_camaras, status_req = 0):
    now = datetime.now()

    fecha_str = str(now.year) + str(now.month) + str(now.day) + '_' + str(now.hour) + str(now.minute)

    output_list = []

    logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s %(message)s')

    # For each camera
    for camera in lista_camaras:

        address = camera[1]
        status = int(camera[5])
        name = settings.OUTPUT_DIR + camera[0] + '_' + fecha_str + '.jpg'

        if status <= status_req:
            # conectarte a la url
            response = requests.get(address, settings.headers)

            # gestion de errores HTTP

            if response.status_code >= 500:  # Server error
                # cambiar el estatus de la camara
                status += 100
                logging.error('Camera: %s Feed %s da un error %s. Nuevo Status: %s', camera[0], camera[1],
                              response.status_code, status)

            elif response.status_code >= 400:  # Client Error
                status += 10
                logging.error('Camera: %s Feed %s da un error %s.  Nuevo Status: %s', camera[0], camera[1],
                              response.status_code, status)

            elif response.status_code == 200:
                # ver si es imagen

                try:
                    img = Image.open(BytesIO(response.content))
                    # resize file
                    img = img.resize((settings.OUTPUT_RESOLUTION_WIDTH, settings.OUTPUT_RESOLUTION_HEIGHT),
                                     Image.ANTIALIAS)
                    # save image
                    img.save(name, settings.IMG_OUTPUT_FORMAT)

                    if status > 0:
                        status = status - 1

                    logging.info('Camera: %s Feed %s graba la imagen Nuevo Status: %s', camera[0], camera[1], status)

                except IOError:
                    status += 1  # archivo no es imagen
                    logging.error('Camera: %s Feed %s, no es una imagen válida. Nuevo Status: %s', camera[0], camera[1], status)

        e = [camera[0], camera[1], camera[2], camera[3], camera[4], str(status)]

        output_list.append(e)

    # TODO grabar archivo para la proxima vez que hagas el crawl

    return output_list



#todo

def obtener_listado_camaras():
    pass


def scrap_video():
    pass

def leer_listado_camaras():
    pass


if __name__ == '__main__':


    crawl()
