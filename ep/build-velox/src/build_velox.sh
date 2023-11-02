#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -exu
#Set on run gluten on S3
ENABLE_S3=OFF
#Set on run gluten on HDFS
ENABLE_HDFS=OFF
BUILD_TYPE=release
VELOX_HOME=""
ENABLE_EP_CACHE=OFF
ENABLE_BENCHMARK=OFF
RUN_SETUP_SCRIPT=ON

OS=`uname -s`
ARCH=`uname -m`

for arg in "$@"; do
  case $arg in
  --velox_home=*)
    VELOX_HOME=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_s3=*)
    ENABLE_S3=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_hdfs=*)
    ENABLE_HDFS=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_type=*)
    BUILD_TYPE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --enable_ep_cache=*)
    ENABLE_EP_CACHE=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --build_benchmarks=*)
    ENABLE_BENCHMARK=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  --run_setup_script=*)
    RUN_SETUP_SCRIPT=("${arg#*=}")
    shift # Remove argument name from processing
    ;;
  *)
    OTHER_ARGUMENTS+=("$1")
    shift # Remove generic argument from processing
    ;;
  esac
done

function apply_compilation_fixes {
  current_dir=$1
  velox_home=$2
  sudo cp ${current_dir}/modify_velox.patch ${velox_home}/
  sudo cp ${current_dir}/modify_arrow.patch ${velox_home}/third_party/
  cd ${velox_home}
  git apply modify_velox.patch
  if [ $? -ne 0 ]; then
    echo "Failed to apply compilation fixes to Velox: $?."
    exit 1
  fi
}

function compile {
  TARGET_BUILD_COMMIT=$(git rev-parse --verify HEAD)

  if [ -z "${GLUTEN_VCPKG_ENABLED:-}" ] && [ $RUN_SETUP_SCRIPT == "ON" ]; then
    if [ $OS == 'Linux' ]; then
      setup_linux
    elif [ $OS == 'Darwin' ]; then
      setup_macos
    else
      echo "Unsupport kernel: $OS"
      exit 1
    fi
  fi

  COMPILE_OPTION="-DVELOX_ENABLE_PARQUET=ON"
  if [ $ENABLE_BENCHMARK == "OFF" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_BUILD_TESTING=OFF -DVELOX_BUILD_TEST_UTILS=ON"
  fi
  if [ $ENABLE_HDFS == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_HDFS=ON"
  fi
  if [ $ENABLE_S3 == "ON" ]; then
    COMPILE_OPTION="$COMPILE_OPTION -DVELOX_ENABLE_S3=ON"
  fi

  COMPILE_OPTION="$COMPILE_OPTION -DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
  COMPILE_TYPE=$(if [[ "$BUILD_TYPE" == "debug" ]] || [[ "$BUILD_TYPE" == "Debug" ]]; then echo 'debug'; else echo 'release'; fi)
  echo "COMPILE_OPTION: "$COMPILE_OPTION

  if [ $ARCH == 'x86_64' ]; then
    make $COMPILE_TYPE EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  elif [[ "$ARCH" == 'arm64' || "$ARCH" == 'aarch64' ]]; then
    CPU_TARGET=$ARCH make $COMPILE_TYPE EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  else
    echo "Unsupport arch: $ARCH"
    exit 1
  fi

  export simdjson_SOURCE=BUNDLED
  make $COMPILE_TYPE EXTRA_CMAKE_FLAGS="${COMPILE_OPTION}"
  # Install deps to system as needed
  if [ -d "_build/$COMPILE_TYPE/_deps" ]; then
    cd _build/$COMPILE_TYPE/_deps
    if [ -d xsimd-build ]; then
      echo "INSTALL xsimd."
      if [ $OS == 'Linux' ]; then
        sudo cmake --install xsimd-build/
      elif [ $OS == 'Darwin' ]; then
        cmake --install xsimd-build/
      fi
    fi
    if [ -d gtest-build ]; then
      echo "INSTALL gtest."
      if [ $OS == 'Linux' ]; then
        sudo cmake --install gtest-build/
      elif [ $OS == 'Darwin' ]; then
        cmake --install gtest-build/
      fi
    fi
  fi
}

function check_commit {
  if [ $ENABLE_EP_CACHE == "ON" ]; then
    if [ -f ${VELOX_HOME}/velox-commit.cache ]; then
      CACHED_BUILT_COMMIT="$(cat ${VELOX_HOME}/velox-commit.cache)"
      if [ -n "$CACHED_BUILT_COMMIT" ]; then
        if [ "$TARGET_BUILD_COMMIT" = "$CACHED_BUILT_COMMIT" ]; then
          echo "Velox build of commit $TARGET_BUILD_COMMIT was cached."
          exit 0
        else
          echo "Found cached commit $CACHED_BUILT_COMMIT for Velox which is different with target commit $TARGET_BUILD_COMMIT."
        fi
      fi
    fi
  else
    git clean -dffx :/
  fi

  if [ -f ${VELOX_HOME}/velox-commit.cache ]; then
    rm -f ${VELOX_HOME}/velox-commit.cache
  fi
}

function setup_macos {
  if [ $ARCH == 'x86_64' ]; then
    ./scripts/setup-macos.sh
  elif [ $ARCH == 'arm64' ]; then
    CPU_TARGET="arm64" ./scripts/setup-macos.sh
  else
    echo "Unknown arch: $ARCH"
  fi
}

function setup_linux {
  local LINUX_DISTRIBUTION=$(. /etc/os-release && echo ${ID})
  local LINUX_VERSION_ID=$(. /etc/os-release && echo ${VERSION_ID})

  if [[ "$LINUX_DISTRIBUTION" == "ubuntu" || "$LINUX_DISTRIBUTION" == "debian" || "$LINUX_DISTRIBUTION" == "pop" ]]; then
    scripts/setup-ubuntu.sh
  elif [[ "$LINUX_DISTRIBUTION" == "centos" ]]; then
    case "$LINUX_VERSION_ID" in
    8) scripts/setup-centos8.sh ;;
    7)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    *)
      echo "Unsupport centos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "alinux" ]]; then
    case "${LINUX_VERSION_ID:0:1}" in
    2)
      scripts/setup-centos7.sh
      set +u
      export PKG_CONFIG_PATH=/usr/local/lib64/pkgconfig:/usr/local/lib/pkgconfig:/usr/lib64/pkgconfig:/usr/lib/pkgconfig:$PKG_CONFIG_PATH
      source /opt/rh/devtoolset-9/enable
      set -u
      ;;
    3) scripts/setup-centos8.sh ;;
    *)
      echo "Unsupport alinux version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  elif [[ "$LINUX_DISTRIBUTION" == "tencentos" ]]; then
    case "$LINUX_VERSION_ID" in
    3.2) scripts/setup-centos8.sh ;;
    *)
      echo "Unsupport tencentos version: $LINUX_VERSION_ID"
      exit 1
      ;;
    esac
  else
    echo "Unsupport linux distribution: $LINUX_DISTRIBUTION"
    exit 1
  fi
}

CURRENT_DIR=$(
  cd "$(dirname "$BASH_SOURCE")"
  pwd
)

if [ "$VELOX_HOME" == "" ]; then
  VELOX_HOME="$CURRENT_DIR/../build/velox_ep"
fi

echo "Start building Velox..."
echo "CMAKE Arguments:"
echo "VELOX_HOME=${VELOX_HOME}"
echo "ENABLE_S3=${ENABLE_S3}"
echo "ENABLE_HDFS=${ENABLE_HDFS}"
echo "BUILD_TYPE=${BUILD_TYPE}"

cd ${VELOX_HOME}
TARGET_BUILD_COMMIT="$(git rev-parse --verify HEAD)"
if [ -z "$TARGET_BUILD_COMMIT" ]; then
  echo "Unable to parse Velox commit: $TARGET_BUILD_COMMIT."
  exit 0
fi
echo "Target Velox commit: $TARGET_BUILD_COMMIT"

check_commit
apply_compilation_fixes $CURRENT_DIR $VELOX_HOME
compile

echo "Successfully built Velox from Source."
echo $TARGET_BUILD_COMMIT >"${VELOX_HOME}/velox-commit.cache"