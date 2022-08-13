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

region=$2
if [ "$region" == "" ]; then
  select_region
else
  if ! verify_region ${region}; then
    echo "Invalid region : ${region}"; exit
  fi
fi


aws_lambda_config=${func}/aws-lambda-tools.${region}.json
if [ -f "${aws_lambda_config}" ]; then
  dotnet lambda deploy-function -cfg ${aws_lambda_config}  
else
  tput setaf 1 && echo "aws lambda config file does net exits" && tput sgr 0
fi

read -t 5 -n1 -r -p "Press any key to continue..."; echo