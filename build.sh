#!/bin/bash

PNAME=fileConvert
VERSION=1.2.1

rm -rf target
mkdir target

SCRATCH_DIR="$PNAME-$VERSION"

mvn dependency:copy-dependencies -DoutputDirectory=target/lib
mvn package

if [ $? -ne 0 ]; then
	exit 1
fi

cp fileConvert target/
cp help target/

cd target
rm -rf $SCRATCH_DIR
mkdir $SCRATCH_DIR

# 在这里将需要发布的文件，放到scratch目录下
cp  rcfile-1.0.jar $SCRATCH_DIR
cp -r lib $SCRATCH_DIR/
cp fileConvert $SCRATCH_DIR/
cp help $SCRATCH_DIR/

CENTOS_VERSION_MAJOR=`lsb_release -a |grep Release |awk '{print $2}'|cut -b1`
#tar czf $SCRATCH_DIR.tar.gz $SCRATCH_DIR
fpm -s dir -t rpm -n $PNAME -v $VERSION --iteration "1.el$CENTOS_VERSION_MAJOR" --epoch=`date +%s` --rpm-defattrfile=0755 --prefix=/usr/local/domob/prog.d $SCRATCH_DIR

if [ $? -ne 0 ]; then
	exit 2
fi

