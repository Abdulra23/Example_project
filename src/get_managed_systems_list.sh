#!/bin/bash

# --------------------------------------------------------------
# Script to get managed systems group list data and output results 
#  to a CSV file.
# 
# Run this script in the ITM environment from the bash/terminal,
#  for example from: /opt/IBM/ITM/bin/ 
#  using the following command.
#  ./get_managed_system_list.sh > distribution.csv 
# 
# --------------------------------------------------------------
# It applies the following commands
# ./tacmd listsystemlist > /tmp/listsystemlist.txt
#  for each entry e.g.
# ./tacmd viewsystemlist -l *LINUX_SYSTEM -e /tmp/16mayLinux.xml
# --------------------------------------------------------------


#echo "Getting listsystemlist data"
echo "Group_Name,Type,Managed_Systems_List "
list_system_list=($(/opt/IBM/ITM/bin/tacmd listsystemlist | awk '{print $1;}'))

for managed_system in "${list_system_list[@]}" ; do
    if [ "${managed_system:0:1}" != '*' ] && [ "${managed_system}" != "NAME" ]; then
        managed_system_detail=$(/opt/IBM/ITM/bin/tacmd viewsystemlist -l "${managed_system}" | grep ^Assigned | awk '{print $4;}')
        managed_system_type=$(/opt/IBM/ITM/bin/tacmd viewsystemlist -l "${managed_system}" | grep ^Type | cut -f 2 -d ':' | sed -e 's/^[[:space:]]*//')
    	echo "${managed_system},${managed_system_type},\"${managed_system_detail}\""
    fi
done

 
