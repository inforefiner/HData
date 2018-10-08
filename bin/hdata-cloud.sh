#!/usr/bin/env bash

if [ -z "$HDATA_HOME" ]
then
  HDATA_HOME=`pwd`
fi

HDATA_CLOUD_DIR=$HDATA_HOME/cloud

if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=`which java`
fi

if [ ! -x "$JAVA" ]; then
    echo "Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"
    exit 1
fi

HDATA_CLASSPATH='.'
for f in $HDATA_CLOUD_DIR/*.jar; do
    HDATA_CLASSPATH=${HDATA_CLASSPATH}:$f;
done

export HADOOP_CONF_DIR=$HADOOP_CONF_DIR
echo "HADOOP_CONF_DIR = $HADOOP_CONF_DIR"

export HADOOP_USER_NAME=$HADOOP_USER_NAME
echo "HADOOP_USER_NAME = $HADOOP_USER_NAME"

MAIN_CLASS="com.inforefiner.hdata.SubmitClient"

#echo $JAVA_OPTS
exec "$JAVA" -cp "$HDATA_CLASSPATH" $MAIN_CLASS "$@"