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

mkdir -p "$original_dir"/website/versioned_docs/version-0.x
mv zio-connect-docs/target/mdoc/* "$original_dir"/website/versioned_docs/version-0.x

mkdir -p "$original_dir"/website/static/api/0.x
mv website/static/api "$original_dir"/website/static/api-0.x

# Now we need to checkout the branch that originally has triggered the site build
cd "$original_dir"
git fetch --tags
ZIO_CONNECT_LATEST_2=`git describe --tags --abbrev=0 ` sbt docs/unidoc
ZIO_CONNECT_LATEST_2=`git describe --tags --abbrev=0 ` sbt docs/mdoc

cd website 
rm -Rf node_modules
yarn install 
yarn build 

cd $OLDDIR
