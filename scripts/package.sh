#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD=`pwd`

cd $DIR
rm -rf OSTranslatorII
rm -f OSTranslatorII.zip

mkdir -p OSTranslatorII/gfs

cd OSTranslatorII

cp ../../OSTranslatorII/*.py .
cp ../../OSTranslatorII/osTrans_128px.png .
cp ../../OSTranslatorII/metadata.txt .
cp ../../OSTranslatorII/*.ui .
cp ../../OSTranslatorII/gfs/* gfs/

cd ..
zip -r OSTranslatorII.zip OSTranslatorII/*

rm -rf OSTranslatorII

cd $PWD