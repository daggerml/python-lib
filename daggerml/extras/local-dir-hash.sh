#!/bin/bash

# setup {{{
path=.
usage() {
  cat <<EOT
usage: $0 [opts]

if path is specified, cd to path, execute

params
------
-h     show this help message
-p     path of directory (default "$path")
EOT
}

while getopts "p:h" o; do
  case "${o}" in
    h) usage && exit 0;;
    p) path=$OPTARG;;
    ?) usage && exit 1;;
  esac
done
shift $((OPTIND-1))
# }}}

# accounts for file names, contents, and permissions
# from https://stackoverflow.com/a/545413

bad_path() {
  echo "bad path $path!" >&2
  exit 1
}

[[ -d $path ]] || bad_path

(cd $path && \
  find . -type f -print0  | sort -z | xargs -0 sha256sum;
  find . \( -type f -o -type d \) -print0 | sort -z | xargs -0 stat -c '%n %a') \
    | sha256sum | sed -E 's/\s+-\s*//'
