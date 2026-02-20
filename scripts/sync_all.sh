#!/bin/bash
# Full sync for HK and US profiles
# Usage: ./scripts/sync_all.sh

LOG_FILE="sync_full.log"

echo "Starting full sync at $(date)" > $LOG_FILE

# 1. Sync Profiles (A/H/US)
echo "Syncing profiles..." >> $LOG_FILE
python3 scripts/sync_profiles_fast.py --market HK --workers 5 --skip-existing >> $LOG_FILE 2>&1
python3 scripts/sync_profiles_fast.py --market US --workers 5 --skip-existing >> $LOG_FILE 2>&1

echo "Full sync completed at $(date)" >> $LOG_FILE
