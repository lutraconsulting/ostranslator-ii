#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PWD=`pwd`

cd $DIR
rm -rf OSTranslatorII
rm -f OSTranslatorII.zip

mkdir -p OSTranslatorII/gfs
mkdir -p OSTranslatorII/ui
mkdir -p OSTranslatorII/images

cd OSTranslatorII

cp ../../OSTranslatorII/*.py .
cp ../../OSTranslatorII/metadata.txt .
cp ../../OSTranslatorII/ui/*.ui ui/
cp ../../OSTranslatorII/gfs/* gfs/
cp ../../OSTranslatorII/images/*.png images/

cd ..
zip -r OSTranslatorII.zip OSTranslatorII/*

rm -rf OSTranslatorII

cd $PWD