#!/bin/sh

set -e

rm -rf $TESTKIT_CHECKOUT_PATH
git clone $TESTKIT_URL $TESTKIT_CHECKOUT_PATH
cd $TESTKIT_CHECKOUT_PATH
git checkout $TESTKIT_VERSION

echo "Testkit args: '$TESTKIT_ARGS'"
python main.py $TESTKIT_ARGS
