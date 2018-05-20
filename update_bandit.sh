cd /home/alaktionov/multiarmed-bandit

now=$(date +"%Y_%m_%d__%H_%M_%S")

echo 'Updating bandit'
echo $now


logfile=/home/alaktionov/logs/bandit/update_bandit_$now.txt

set -e

/anaconda3/bin/python job.py --update 1 &>> $logfile

echo 'Fuck yeah!'

