#!/bin/bash

# This does a regex replace of all cereal includes with the new location inside
# the package.

find . -exec sed -i -e 's#tidas/cereal/#tidas/tidas/cereal/#g' '{}' \;
