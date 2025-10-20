#!/bin/bash

source gradle.properties

graal_dir=.graalvm



[[ $(uname -a) =~ Darwin ]] && os=darwin || os=linux

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

if [[ ! -d ${graal_dir}/graalvm-community-jdk-${graalvmVersion}_macos-x64_bin.tar.gz ]]
  then
    graalvmDist=graalvm-community-jdk-${graalvmVersion}_macos-x64_bin.tar.gz
    echo "Installing GraalVM: ${graalvmDist}"
    mkdir -p ${graal_dir}
    pushd ${graal_dir}
    curl -OL -A "Mozilla Chrome Safari" https://github.com/graalvm/graalvm-ce-builds/releases/download/jdk-${graalvmVersion}/${graalvmDist}
    tar xf ${graalvmDist}
    echo $graal_dir
    popd
    setUpEnvironmentVariables
  else 
    echo "GraalVM already installed"
fi
