#!/usr/bin/env bash

for dir in data/*
do
  echo "Processing dir " $dir
  for filename in $dir/*
  do
    if [[ $filename =~ '.tar.gz' ]]; then
      echo "Extracting " $filename
      tar -xf $filename -C ./extracted
    fi
  done;
done;