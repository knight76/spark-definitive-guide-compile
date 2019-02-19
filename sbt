#!/usr/bin/env bash

sbtver=1.2.8
sbtjar=sbt-launch.jar
sbtsha128=073c169c6e1a47b8ae78a7a718b907424dedab30

sbtrepo="https://repo1.maven.org/maven2/org/scala-sbt/sbt-launch"

if [ ! -f $sbtjar ]; then
  echo "downloading $PWD/$sbtjar" 1>&2
  if ! curl --location --silent --fail --remote-name $sbtrepo/$sbtver/$sbtjar; then
    exit 1
  fi
fi

checksum=`openssl dgst -sha1 $sbtjar | awk '{ print $2 }'`
if [ "$checksum" != $sbtsha128 ]; then
  echo "download file checksum is $checksum"
  echo "bad $PWD/$sbtjar.  delete $PWD/$sbtjar and run $0 again."
  exit 1
fi

[ -f ~/.sbtconfig ] && . ~/.sbtconfig

if [ -z "$SBT_OPTS" ]; then
  JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"
  JAVA_OPTS+=" -XX:+AggressiveOpts"
  JAVA_OPTS+=" -XX:+UseParNewGC"
  JAVA_OPTS+=" -XX:+UseConcMarkSweepGC"
  JAVA_OPTS+=" -XX:+CMSParallelRemarkEnabled"
  JAVA_OPTS+=" -XX:+CMSClassUnloadingEnabled"
  JAVA_OPTS+=" -XX:ReservedCodeCacheSize=128m"
  JAVA_OPTS+=" -XX:SurvivorRatio=128"
  JAVA_OPTS+=" -XX:MaxTenuringThreshold=0"
  JAVA_OPTS+=" -Xss8M"
  JAVA_OPTS+=" -Xms512M"
  JAVA_OPTS+=" -Xmx2G"
  JAVA_OPTS+=" -server"
  JAVA_OPTS+=" -Dquill.macro.log=false"
else
  JAVA_OPTS=$SBT_OPTS
fi

java -ea \
  $JAVA_OPTS \
  -jar $sbtjar "$@"
