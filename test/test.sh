#!/bin/bash

ROOTDIR="`dirname $0`/.."
PWD="`pwd`"

cd "${ROOTDIR}"

#----------------------------------------------------------------------
#

mkdir -p test/logs test/out 

if [ $? -ne 0 ] 
then
	echo "Unable to create log and output directories";
	exit 1
fi	 

#----------------------------------------------------------------------
# Test 1:
#

`./gen_client/gen_client -l test/logs/log1 -c test/config test/scripts/test1 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test1 test/sample-out/test1 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 1 failed"
else
    echo "Test 1 ok"
fi 

#------------------------------------------------------------
# Test 2
#

`./gen_client/gen_client -l test/logs/log2 -c test/config test/scripts/test2 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test2 test/sample-out/test2 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 2 failed"
else
    echo "Test 2 ok"
fi 

cd "${PWD}"

#------------------------------------------------------------
# Test 3
#

`./gen_client/gen_client -l test/logs/log3 -c test/config test/scripts/test3 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test3 test/sample-out/test3 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 3 failed"
else
    echo "Test 3 ok"
fi 

#------------------------------------------------------------
# Test 4
#

`./gen_client/gen_client -l test/logs/log4 -c test/config test/scripts/test4 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test4 test/sample-out/test4 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 4 failed"
else
    echo "Test 4 ok"
fi 

#------------------------------------------------------------
# Test 5
#

`./gen_client/gen_client -l test/logs/log5 -c test/config test/scripts/test5 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test5 test/sample-out/test5 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 5 failed"
else
    echo "Test 5 ok"
fi 

#------------------------------------------------------------
# Test 6
#

`./gen_client/gen_client -l test/logs/log6 -c test/config test/scripts/test6 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test6 test/sample-out/test6 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 6 failed"
else
    echo "Test 6 ok"
fi 

#------------------------------------------------------------
# Test 7
#

`./gen_client/gen_client -l test/logs/log7 -c test/config test/scripts/test7 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test7 test/sample-out/test7 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 7 failed"
else
    echo "Test 7 ok"
fi 

#------------------------------------------------------------
# Test 8
#

`./gen_client/gen_client -l test/logs/log8 -c test/config test/scripts/test8 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test8 test/sample-out/test8 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 8 failed"
else
    echo "Test 8 ok"
fi 

#------------------------------------------------------------
# Test 9
#

`./gen_client/gen_client -l test/logs/log9 -c test/config test/scripts/test9 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test9 test/sample-out/test9 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 9 failed"
else
    echo "Test 9 ok"
fi 

#------------------------------------------------------------
# Test 10
#

`./gen_client/gen_client -l test/logs/log10 -c test/config test/scripts/test10 > /dev/null 2>&1`
RET1="$?"

`diff -q test/out/test10 test/sample-out/test10 > /dev/null 2>&1`
RET2="$?"

if [ "$RET1" != "0" -o "$RET2" != "0" ]; then
    echo "Test 10 failed"
else
    echo "Test 10 ok"
fi 


cd "${PWD}"
