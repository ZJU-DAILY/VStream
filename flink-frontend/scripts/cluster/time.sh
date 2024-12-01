#!/bin/bash
echo "Run in sudo!"
read -p "Sleep hours: " waittime
read -p "Confirm: sleep ${waittime}h? " confirmation
if [ "$confirmation" = "y" ]; then
  echo "ok"
else
  echo "abort"
  exit 0
fi
ls ../syslogs
read -p "Folder name: " folder

sleep ${waittime}h

echo -e "$folder\ny" | ./stop.sh
echo "y" | ./start.sh
