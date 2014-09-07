#!/bin/bash

unset M2_HOME MAVEN_OPTS MVN_HOME
MAVEN_HOME=/btoddb/apache-maven-3.2.3

if [ $# -lt 1 ] ; then
  echo
  echo "usage: $0 <build-type> [<skipTests>]"
  echo "       -- build-type = package | install | deploy | release"
  echo
  exit
fi

buildType=$1
if [ $# -gt 1 ] ; then
  skipTests="-DskipTests -DskipITs"
fi

case ${buildType} in
  "install")
    ${MAVEN_HOME}/bin/mvn clean install ${skipTests}
    ;;

  "deploy")
    ${MAVEN_HOME}/bin/mvn clean deploy ${skipTests}
    ;;

  "release")
    ${MAVEN_HOME}/bin/mvn release:clean release:prepare
#    ${MAVEN_HOME}/bin/mvn release:perform
    ;;

  *)
    ${MAVEN_HOME}/bin/mvn clean package ${skipTests}
    ;;
esac

