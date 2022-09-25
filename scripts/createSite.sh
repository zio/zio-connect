set -ex

OLDDIR=`pwd`
export SBT_OPTS="-Xmx2048m -XX:+UseG1GC"

# Clean existing build and mdoc output directory
rm -Rf target
rm -Rf website/docs
rm -Rf website/versioned_docs

# Checkout latest version of the website from the 1.x series

original_dir=$(pwd)
cd /tmp
git clone https://github.com/zio/zio-connect.git
cd zio-connect
sbt docs/mdoc
sbt docs/unidoc

cd website 
rm -Rf node_modules
yarn install 
yarn build 

cd $OLDDIR
