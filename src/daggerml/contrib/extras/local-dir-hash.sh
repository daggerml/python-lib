#!/bin/bash
debug=0
usage() {
  echo usage: $0 [opts] path
}

while getopts "dh" o; do
  case "${o}" in
    h) usage && exit 0;;
    d) debug=1;;
    ?) usage && exit 1;;
  esac
done
shift $((OPTIND-1))
path="$(realpath $1)"

# accounts for file names, contents, and permissions
# from https://stackoverflow.com/a/545413

if [ $debug = 1 ]; then
  (cd $path && \
    find . -type f -print0 | sort -z | xargs -0 sha256sum;
    find . \( -type f -o -type d \) -print0 | sort -z | xargs -0 stat -c '%n %a')
  exit
fi

(cd $path && \
  find . -type f -print0 | sort -z | xargs -0 sha256sum;
  find . \( -type f -o -type d \) -print0 | sort -z | xargs -0 stat -c '%n %a') \
    | sha256sum - | awk '{ print $1 }'
