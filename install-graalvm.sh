#!/bin/bash

source gradle.properties

graal_dir=.graalvm



[[ $(uname -a) =~ Darwin ]] && os=darwin || os=linux

[[ $os == darwin ]] && archive_os=macos || archive_os=linux
graalvmDist=graalvm-community-jdk-${graalvmVersion}_${archive_os}-x64_bin.tar.gz

setUpEnvironmentVariables() {
  echo "Setup GRAALVM_HOME and JAVA_HOME environment variables"  
  if [[ $os == darwin ]]
  then
    export GRAALVM_HOME=${PWD}/${graal_dir}/graalvm-community-openjdk-${graalvmVersion}+11.1/Contents/Home
  else
    export GRAALVM_HOME=${PWD}/${graal_dir}/graalvm-community-openjdk-${graalvmVersion}+11.1
  fi
  export JAVA_HOME=${GRAALVM_HOME}
}

if [[ ! -f ${graal_dir}/${graalvmDist} ]]
  then
    echo "Installing GraalVM: ${graalvmDist}"
    mkdir -p ${graal_dir}
    pushd ${graal_dir}
    curl -OL -A "Mozilla Chrome Safari" https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${graalvmVersion}/${graalvmDist}
    tar xf ${graalvmDist}
    echo $graal_dir
    popd

  else 
    echo "GraalVM already installed"
fi
setUpEnvironmentVariables
