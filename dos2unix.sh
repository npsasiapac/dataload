#!/bin/sh
##############################################################################
# Script to convert all files to unix                                        #
#                                                                            #
# Run from the command line with:                                            #
# sh dos2unix.sh dir_to_convert                                              #
# For example sh dos2unix.sh DataLoadAreas/HEM/20150806                      #
##############################################################################
#Assign Internal Variables                                                   #
##############################################################################
log=$1
rcode=0
LOGFILE_HOME=/app/first/dataload
INSTALL_HOME=${LOGFILE_HOME}/20150915/${1}

set -f                       # Turn off meta substitution while
#file_list=$INSTALL_HOME/"*"  # Change target to ok for trigger.
file_list=20150915/unzip/6.12_HEM_Queensland_v1/"*"  # Change target to ok for trigger.

set +f                    # Turn on meta substitution ready to LOOP.

file_list=`eval echo $file_list`

for FILE in $file_list                     # Process each in turn.
do

   echo "Converting "${FILE}
   dos2unix -q -437 ${FILE} ${FILE}

done


