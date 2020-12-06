#!/usr/bin/env bash

flatc --go flatbuffer.fbs
# Move files to the correct directory.
mv fb/* ./
rmdir fb
