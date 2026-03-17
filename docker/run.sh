#!/bin/bash

# Retrieve from Environment variables, or use 1000 as default
gid=${PGID:-1000}
uid=${PUID:-1000}

! getent group "${gid}" && addgroup -g "${gid}" -S ownfoil
GROUP=$(getent group "${gid}" | cut -d ":" -f 1)
! getent passwd "${uid}" && adduser -u "${uid}" -G "${GROUP}" -S ownfoil

chown -R ${uid}:${gid} /app
chown -R ${uid}:${gid} /root

# Symlink keys.txt as prod.keys for ACORN compatibility
if [ -f /app/config/keys.txt ]; then
    ln -sf /app/config/keys.txt /app/prod.keys
fi

# Ensure combined output directory exists and is writable
if [ -d /combined ]; then
    chown -R ${uid}:${gid} /combined
fi

echo "Starting ownfoil"

exec sudo -E -u "#${uid}" python /app/app.py
