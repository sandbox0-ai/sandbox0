#!/bin/bash

# For debugging: automatically sync local code changes to remote to maintain consistency.
# Prerequisites: rsync must be installed on both local and server; configure the target IP and path.
# Usage: sh fswatch-sh.sh <target>
IGNORE_FILE=".gitignore"
FSWATCH_EXCLUDES=()
FSWATCH_PID=""
RSYNC_PID=""
cleanup() {
    echo "Exiting..."
    [ -n "$FSWATCH_PID" ] && kill "$FSWATCH_PID" >/dev/null 2>&1
    [ -n "$RSYNC_PID" ] && kill "$RSYNC_PID" >/dev/null 2>&1
    exit 0
}
trap cleanup INT TERM
if [ -f "$IGNORE_FILE" ]; then
    while IFS= read -r line; do
        line="${line%%$'\r'}"
        [ -z "$line" ] && continue
        [[ "$line" == \#* ]] && continue
        line="${line#/}"
        regex=$(printf '%s' "$line" | sed -e 's/[.[\^$+?(){|}]/\\&/g' -e 's/\*/.*/g' -e 's#/$#(/|$)#')
        FSWATCH_EXCLUDES+=("-e" "(^|/)$regex")
    done < "$IGNORE_FILE"
fi
while [ "true" ]; do
    echo '----------------------------------------------------------------------------'
    fswatch -r -L -1 "${FSWATCH_EXCLUDES[@]}" * &
    FSWATCH_PID=$!
    wait "$FSWATCH_PID"
    FSWATCH_PID=""
    date
    # Note: rsync needs to be installed on the server
    rsync --delete-before -d -r --progress --filter=':- .gitignore' --exclude='.git/' . "$1" &
    RSYNC_PID=$!
    wait "$RSYNC_PID"
    RSYNC_PID=""
    date
done
