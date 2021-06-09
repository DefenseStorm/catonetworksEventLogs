CatoNetworks Integration for DefenseStorm

to pull this repository and submodules:

git clone --recurse-submodules https://github.com/DefenseStorm/catonetworksEventLogs.git

If this is the first integration on this DVM, Do the following:
cp ds-integration/ds_events.conf to /etc/syslog-ng/conf.d

Edit /etc/syslog-ng/syslog-ng.conf and add local7 to the excluded list for filter f_syslog3 and filter f_messages. The lines should look like the following:

filter f_syslog3 { not facility(auth, authpriv, mail, local7) and not filter(f_debug); };

filter f_messages { level(info,notice,warn) and not facility(auth,authpriv,cron,daemon,mail,news,local7); };

Restart syslog-ng service syslog-ng restart

Copy the template config file and update the settings
cp catonetworksEventLogs.conf.template catonetworksEventLogs.conf

change the required items in the config file based on your configuration.
You need to download the script from Cato to find your START_INDEX (called 'last' in their script
for the first time you run this integration.  If the integration stops running for an extended
period of time, you may need to configure the START_INDEX again.

Add the following entry to the root crontab so the script will run every 5 minutes.

*/5 * * * * cd /usr/local/catonetworksEventLogs; ./catonetworksEventLogs.py
