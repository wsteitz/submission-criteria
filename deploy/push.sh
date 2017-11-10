#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/../

docker build -t 074996771758.dkr.ecr.us-west-2.amazonaws.com/api-ml:$2 --build-arg env=$1 . &&
aws ecr get-login --no-include-email | sudo bash &&
docker push 074996771758.dkr.ecr.us-west-2.amazonaws.com/api-ml:$2 &&
./deploy/gen-dockerrun.sh $2 &&
eb deploy --staged $2
