#!/bin/bash
regions=( 'us-west-2' 'ap-northeast-2' )
funcs=( 'CCU' 'ClanWar' 'DailyCrm' 'ReplayPick' )

verify_region() {
  region=$1
  for i in "${regions[@]}"
  do
    if [[ ${i} = ${region} ]]; then
      return 0
    fi
  done
  return 1
}

select_region() {
  echo "[Select Region]"
  select region in "${regions[@]}"
  do
    [[ -n ${regions} ]] || { echo "Invalid choice." >&2; continue; }
    break
  done
}

verify_func() {
  func=$1
  for i in "${funcs[@]}"
  do
    if [[ ${i} = ${func} ]]; then
      return 0
    fi
  done
  return 1
}

select_func() {
  echo "[Select Function]"
  select func in "${funcs[@]}"
  do
    [[ -n ${func} ]] || { echo "Invalid choice." >&2; continue; }
    break
  done
}
