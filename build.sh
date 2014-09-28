#!/bin/bash

if [ $# -lt 1 ] ; then
  echo
  echo "usage: $0 <build-type> [<skipTests>]"
  echo "       -- build-type = package | verify | install | deploy | release | site | license-check | license-update"
  echo
  exit
fi

unset M2_HOME MAVEN_OPTS MVN_HOME
MAVEN_HOME=/btoddb/apache-maven-3.2.3
MAVEN_CMD_OPTS="--show-version"
MAVEN_CMD="${MAVEN_HOME}/bin/mvn ${MAVEN_CMD_OPTS}"

buildType=$1
if [ $# -gt 1 ] ; then
  skipTests="-DskipTests -DskipITs"
fi

case ${buildType} in
  "verify")
    ${MAVEN_CMD} clean verify ${skipTests}
    ;;

  "install")
    ${MAVEN_CMD} clean install ${skipTests}
    ;;

  "deploy")
    ${MAVEN_CMD} clean deploy ${skipTests}
    ;;

  "release")
    ${MAVEN_CMD} -P release release:clean release:prepare release:perform
    ;;

  "site")
    ${MAVEN_CMD} site ${skipTests}
    ;;

  "license-check")
    ${MAVEN_CMD} license:check-file-header
    ;;

  "license-update")
    ${MAVEN_CMD} license:update-file-header
    ;;

  *)
    ${MAVEN_CMD} ${MAVEN_CMD_OPTS} clean package ${skipTests}
    ;;
esac

