#!/usr/bin/env bash
for filename in extracted/*
  do
    php push.php $filename

done;
