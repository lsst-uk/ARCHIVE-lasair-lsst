# Sherlock Wrapper
The Sherlock Kafka wrapper consumes alerts (in plain JSON) from an input topic, sends them to Sherlock,
adds the Sherlock classification and crossmatches back into the alert and republishes on the output topic.

wrapper_runner is used to launch the wrapper, copy any log messages marked CRITICAL or ERROR to Slack, and attempt to restart on failure (with exponential backoff to avoid flooding Slack with messages).

## Build
The Ansible script deploys a Docker image. To build a new image use the Dockerfile then tag the image, push it to a repository and then edit the Ansible script to reference it.

At present the version string in wrapper.py and the version number of the docker image should match, e.g. image gpfrancis/sherlock-wrapper:0.5.16 contains wrapper.py version 0.5.16.

## Deployment
Use the Ansible script in ansible/sherlock-wrapper.yml

This script has a single parameter for the Sherlock database host. To deploy with different databases for different wrapper hosts use the "-l" switch to limit ansible-playbook to the correct host(s), then edit the yml file and re-run for the other host(s). If the wrapper and database are on the same host then this is not necessary as the database host can simply be set to localhost.

## Notes
These notes apply to the current deployment as of 13/7/21.

* Sherlock wrappers run on 2 instances with IPs of 192.168.0.9 and 192.168.0.21
* 3 wrapper containers run on each instance
* The Ansible scripts in use are located at 192.168.0.9:~/lasair-lsst/ansible

If anything goes wrong, start by running the Ansible script again.
* Check that the sherlock database is set correctly for the host you are deploying to in sherlock-wrapper.yml (currently 192.168.0.43 for 0.9 and 0.14 for 0.21)
* Limit to the specific host you want to using the -l flag

Example:
```
ubuntu@sherlock4:~/lasair-lsst/ansible$ export VAULT_ADDR=https://vault.lsst.ac.uk
ubuntu@sherlock4:~/lasair-lsst/ansible$ export VAULT_TOKEN=**********
ubuntu@sherlock4:~/lasair-lsst/ansible$ ansible-playbook -i hosts.yaml -l 192.168.0.21 sherlock-wrapper.yml 

PLAY [sherlocks] *******************************************************************************************************************************************************************************

TASK [Gathering Facts] *************************************************************************************************************************************************************************
ok: [192.168.0.21]

TASK [Create /opt/lasair directory] ************************************************************************************************************************************************************
ok: [192.168.0.21]

TASK [Deploy Sherlock config] ******************************************************************************************************************************************************************
ok: [192.168.0.21]

TASK [Deploy Sherlock wrapper config] **********************************************************************************************************************************************************
ok: [192.168.0.21]

TASK [Deploy Sherlock wrapper runner config] ***************************************************************************************************************************************************
ok: [192.168.0.21]

TASK [Deploy Docker compose file] **************************************************************************************************************************************************************
changed: [192.168.0.21]

TASK [Start Sherlock wrapper service] **********************************************************************************************************************************************************
changed: [192.168.0.21]

PLAY RECAP *************************************************************************************************************************************************************************************
192.168.0.21               : ok=7    changed=2    unreachable=0    failed=0   
```

There should now be three containers running on the target instance:
```
ubuntu@sherlock3:~$ sudo docker ps -a
CONTAINER ID        IMAGE                               COMMAND                  CREATED              STATUS              PORTS               NAMES
65c75ce6871e        gpfrancis/sherlock-wrapper:0.5.16   "/usr/bin/python3 /w…"   About a minute ago   Up About a minute                       sherlock_wrapper_sherlock_wrapper_2
df55addb50de        gpfrancis/sherlock-wrapper:0.5.16   "/usr/bin/python3 /w…"   About a minute ago   Up About a minute                       sherlock_wrapper_sherlock_wrapper_3
105eab9e4734        gpfrancis/sherlock-wrapper:0.5.16   "/usr/bin/python3 /w…"   About a minute ago   Up About a minute                       sherlock_wrapper_sherlock_wrapper_1
```

We can view the logs like this:
```
ubuntu@sherlock3:~$ sudo docker logs 65c75ce6871e --tail 20
Starting Sherlock wrapper.
2021-07-13 10:16:54,185:INFO_:wrapper.py:subscribing to topic ztf_ingest
2021-07-13 10:16:59,238:INFO_:wrapper.py:consumed 790 alerts
2021-07-13 10:16:59,338:INFO_:wrapper.py:running Sherlock classifier on 604 objects
2021-07-13 10:17:09,041:INFO_:wrapper.py:got 604 classifications
2021-07-13 10:17:09,041:INFO_:wrapper.py:got 570 crossmatches
/wrapper.py:271: DeprecationWarning: PY_SSIZE_T_CLEAN will be required for '#' formats
  p.produce(conf['output_topic'], value=json.dumps(alert))
2021-07-13 10:17:14,390:INFO_:wrapper.py:produced 790 alerts
2021-07-13 10:17:14,952:INFO_:wrapper.py:consumed 790 alerts
2021-07-13 10:17:15,019:INFO_:wrapper.py:running Sherlock classifier on 674 objects
2021-07-13 10:18:30,514:INFO_:wrapper.py:got 674 classifications
2021-07-13 10:18:30,514:INFO_:wrapper.py:got 648 crossmatches
/wrapper.py:271: DeprecationWarning: PY_SSIZE_T_CLEAN will be required for '#' formats
  p.produce(conf['output_topic'], value=json.dumps(alert))
2021-07-13 10:18:36,404:INFO_:wrapper.py:produced 790 alerts
2021-07-13 10:18:37,003:INFO_:wrapper.py:consumed 790 alerts
2021-07-13 10:18:37,084:INFO_:wrapper.py:running Sherlock classifier on 715 objects
```

The wrapper itself (wrapper.py) is run by a launcher (wrapper_runner.py) that will send alerts to Slack and attempt to restart it on failure so restarting containers should not normally be necessary. However, if a container does stop for some reason then you can try restarting it with `docker container start <ID>`. If the container is running, but it seems like a restart might help then the command is `docker container restart <ID>`. The most likely reason for wanting to force a restart is an external error (e.g. database being down) that is now fixed and you don't want to wait for the next auto restart attempt.

Beware of issue #86
