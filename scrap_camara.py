#!/usr/bin/python

import sys, getopt
import argparse

import requests
import settings
from PIL import Image
from io import BytesIO

from datetime import datetime

import csv

import logging


def crawl(lista_camaras = settings.lista_camaras, status_req = 0):
    now = datetime.now()
    fecha_str = str(now.year) + str(now.month) + str(now.day) + '_' + str(now.hour) + str(now.minute)

    output_list = []

    logging.basicConfig(filename='crawler.log', level=logging.INFO, format='%(asctime)s %(message)s')

    errores = 0
    adquiridos = 0

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
                errores += 1
                logging.error('Camera: %s Feed %s da un error %s. Nuevo Status: %s', camera[0], camera[1],
                              response.status_code, status)

            elif response.status_code >= 400:  # Client Error
                status += 10
                errores += 1
                logging.error('Camera: %s Feed %s da un error %s.  Nuevo Status: %s', camera[0], camera[1],
                              response.status_code, status)

            elif response.status_code == 200:
                # ver si es imagen

                try:
                    img = Image.open(BytesIO(response.content))
                    # resize file
                    # Probablemente se tenga que hacer en otro proceso
                    #img = img.resize((settings.OUTPUT_RESOLUTION_WIDTH, settings.OUTPUT_RESOLUTION_HEIGHT),
                    #                 Image.ANTIALIAS)
                    # save image
                    img.save(name, settings.IMG_OUTPUT_FORMAT)
                    adquiridos += 1

                    if status > 0:
                        status = status - 1

                    logging.info('Camera: %s Feed %s graba la imagen Nuevo Status: %s', camera[0], camera[1], status)

                except IOError:
                    status += 1  # archivo no es imagen
                    errores += 1
                    logging.error('Camera: %s Feed %s, no es una imagen válida. Nuevo Status: %s', camera[0], camera[1], status)

        e = [camera[0], camera[1], camera[2], camera[3], camera[4], str(status)]

        output_list.append(e)

    # TODO grabar archivo para la proxima vez que hagas el crawl

    return output_list, errores, adquiridos


def leer_listado_camaras(file_name="lista_camaras.csv"):

    with open(file_name, newline='') as file:
        reader = csv.reader(file)
        l = list(map(tuple, reader))
    return l


def scrap_video():
    # TODO De momento no hay nada, si sobra tiempo puedo intentar hacerlo

    pass


def actualizar_listado_camaras(l, file_name = "lista_camaras1.csv"):
    with open(file_name, 'w', newline='\n') as myfile:
        wr = csv.writer(myfile, lineterminator='\n')
        for e in l:
            wr.writerow(e)


def main():
    logging.basicConfig(filename='scrap_camara.log', level=logging.INFO, format='%(asctime)s %(message)s')

    descripcion = """Esta es la ayuda del script"""

    parser = argparse.ArgumentParser(description=descripcion)
    parser.add_argument("-V", "--version", help="show program version", action="store_true")
    parser.add_argument("-I", "--input", help="CSV file with list of cameras")
    parser.add_argument("-O", "--output", help="Output CSV file with list of cameras with updated success level")
    parser.add_argument("-s", "--status_requested", help="status requested (0, las que responden siempre)")
    parser.add_argument("-v", "--verbose", help="", action="store_true")

    # read arguments from the command line
    args = parser.parse_args()

    # check for --version or -V
    if args.version:
        print("Scrap Camaras de Open Data Madrid, version", settings.version)

    if args.input:
        inputfile = args.input
        #todo comprobar que existe el fichero

    else:
        inputfile = 'lista_camaras.csv'

    if args.output:
        outputfile = args.output
    elif args.input:
        outputfile = inputfile
    else:
        outputfile = 'lista_camaras_test_out.csv'

    if args.status_requested:
        status_requested = int(args.status_requested)
    else:
        status_requested = 0

    if args.verbose:
        verbose = True
    else:
        verbose = False

    inicio = datetime.now()

    if verbose:
        print('Input file is', inputfile)
        print('Output file is', outputfile)
        print('status requested is', status_requested)
        print('Inicio de la operación', inicio)

    lista_camara = leer_listado_camaras(inputfile)

    # lanza el crawler y devuelve una lista actualizada
    lista_camara_actualizada, errores, adquiridos = crawl(lista_camaras=lista_camara, status_req=status_requested)

    resultado_crawl = "De %s pedidos, se han adquirido %s imagenes, con %s errores" %(len(lista_camara), adquiridos, errores)

    if verbose:
        print(resultado_crawl)

    if errores:
        logging.warning(resultado_crawl)
    else:
        logging.info(resultado_crawl)

    # escribe la lista actualizada para el siguiente crawl
    actualizar_listado_camaras(lista_camara_actualizada, file_name=outputfile)

    fin = datetime.now()
    duracion = fin - inicio

    logging.info('Me ha costado hacer el crawl %s' %(str(duracion)))
    if verbose:
        print('Fin de la operación',fin)
        print('Duracion', duracion)


if __name__ == '__main__':
    #main(sys.argv[1:])
    main()
