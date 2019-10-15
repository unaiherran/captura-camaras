#!/usr/bin/python

import glob, os
import argparse
from time import sleep

import settings

import boto3
from botocore.client import Config

from utils import setup_logger


def is_processed(filename):
    return True


def process_files(verbose):

    process_logger = setup_logger('process_log', 'process_image.log')
    directorio = settings.SCRAP_DIR + '*'
    longitud_scrap_dir = len(settings.SCRAP_DIR)

    # setting Amazon Bucket
    s3 = boto3.resource('s3',
                        config=Config(signature_version='s3v4')
                        )
    bucket_name = 'proyecto-bd3-ff'

    for file in glob.glob(directorio):
        file_name = file[longitud_scrap_dir:]

        # Mirar si esta ya procesado (Hashes y demas)

        if not is_processed(file):
            # # copiar a amazon
            # data = open(file, "rb")
            # key = 'camera_images/'+file_name
            # s3.Bucket(bucket_name).put_object(Key=key, Body=data)
            # process_logger.info('%s subido a bucket %s/camera_images' %(file_name, bucket_name))
            #
            # # mover a directorio de procesado
            # destino = settings.PROCCESED_DIR + file_name
            # os.rename(file, destino)
            # process_logger.info('%s movido a processed/%s' %(file, file_name))
            pass
        # else:
        #     # borrar archivo para no procesarlo otra vez)
        #     os.remove(file)
        #     process_logger.info('%s ya estaba procesado, asi que lo borro' %file)



def main():
    descripcion = """Esta es la ayuda del script"""

    to_s3_logger = setup_logger('to_s3_log', 'to_s3.log')

    parser = argparse.ArgumentParser(description=descripcion)
    parser.add_argument("-V", "--version", help="show program version", action="store_true")
    parser.add_argument("-v", "--verbose", help="", action="store_true")

    # read arguments from the command line
    args = parser.parse_args()

    # check for --version or -V
    if args.version:
        print("Send to S3 Camaras de Open Data Madrid, version", settings.version_a_s3)

    if args.verbose:
        verbose = True
    else:
        verbose = False


    #while True:
    process_files(verbose=verbose)
    sleep(1)

if __name__ == '__main__':
    main()
