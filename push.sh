#!/usr/bin/env bash
for filename in extracted/*
  do
    php push.php $filename
    rm -f $filename
done;
