set -ex

export SBT_OPTS="-Xmx2048m -XX:+UseG1GC"

# Clean existing build and mdoc output directory
rm -Rf target
rm -Rf website/docs
rm -Rf website/versioned_docs

sbt docs/mdoc
sbt docs/unidoc

cd website
rm -Rf node_modules
yarn install
yarn build

