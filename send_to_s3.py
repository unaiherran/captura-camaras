#!/usr/bin/python

import glob, os, sys
import argparse
from time import sleep

import settings

import boto3
from botocore.client import Config

import hashlib
from utils import setup_logger


def chunk_reader(fobj, chunk_size=1024):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
    hashobj = hash()
    file_object = open(filename, 'rb')

    if first_chunk_only:
        hashobj.update(file_object.read(1024))
    else:
        for chunk in chunk_reader(file_object):
            hashobj.update(chunk)
    hashed = hashobj.digest()

    file_object.close()
    return hashed


def check_for_duplicates(paths, hash=hashlib.sha1):
    hashes_by_size = {}
    hashes_on_1k = {}
    hashes_full = {}

    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    # if the target is a symlink (soft one), this will
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except (OSError,):
                    # not accessible (permissions, etc) - pass on
                    continue

                duplicate = hashes_by_size.get(file_size)

                if duplicate:
                    hashes_by_size[file_size].append(full_path)
                else:
                    hashes_by_size[file_size] = []  # create the list for this file size
                    hashes_by_size[file_size].append(full_path)

    # For all files with the same file size, get their hash on the 1st 1024 bytes
    for __, files in hashes_by_size.items():
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
            except (OSError,):
                # the file access might've changed till the exec point got here
                continue

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    for __, files in hashes_on_1k.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpy cycles on it

        for filename in files:
            try:
                full_hash = get_hash(filename, first_chunk_only=False)
            except (OSError,):
                # the file access might've changed till the exec point got here
                continue

            duplicate = hashes_full.get(full_hash)
            if duplicate:
                print("Duplicate found: %s and %s" % (filename, duplicate))
            else:
                hashes_full[full_hash] = filename


def is_processed(filename):
    # Basado en https://stackoverflow.com/questions/748675/finding-duplicate-files-and-removing-them

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
            # copiar a amazon
            data = open(file, "rb")
            key = 'camera_images/'+file_name
            # s3.Bucket(bucket_name).put_object(Key=key, Body=data)
            mensaje = '%s subido a bucket %s/camera_images' %(file_name, bucket_name)
            process_logger.info(mensaje)
            if verbose:
                print(mensaje)

            # mover a directorio de procesado
            destino = settings.PROCCESED_DIR + file_name
            # os.rename(file, destino)
            mensaje = '%s movido a processed/%s' %(file, file_name)
            process_logger.info(mensaje)
            if verbose:
                print(mensaje)

        else:
            # borrar archivo para no procesarlo otra vez)
            #  os.remove(file)
            mensaje = '%s ya estaba procesado, asi que lo borro' %file
            process_logger.info(mensaje)
            if verbose:
                print(mensaje)


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
