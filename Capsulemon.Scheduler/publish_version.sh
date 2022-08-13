#!/bin/bash
. ./lambda_common.sh

func=$1
func=${func/\//}
if [ "$func" == "" ]; then
  select_func
else
  if ! verify_func ${func}; then
    echo "Invalid func : ${func}"; exit
  fi
fi
funcname=Scheduler-${func}

region=$2
if [ "$region" == "" ]; then
  select_region
else
  if ! verify_region ${region}; then
    echo "Invalid region : ${region}"; exit
  fi
fi

alias_=live
Revision=`git log -1 --oneline . | awk {'print $1'}`
version=$(aws lambda publish-version --function-name ${funcname} --description ${Revision} --region ${region} --query Version --output text)
echo version: ${version}
aws lambda update-alias --function-name ${funcname} --name ${alias_} --function-version ${version} --description ${Revision} --region ${region} ||
aws lambda create-alias --function-name ${funcname} --name ${alias_} --function-version ${version} --description ${Revision} --region ${region}

read -t 5 -n1 -r -p "Press any key to continue..."; echo