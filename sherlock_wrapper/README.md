# Sherlock Wrapper
The Sherlock Kafka wrapper consumes alerts (in plain JSON) from an input topic, sends them to Sherlock,
adds the Sherlock classification and crossmatches back into the alert and republishes on the output topic.

wrapper_runner is used to launch the wrapper, copy any log messages marked CRITICAL or ERROR to Slack, and attempt to restart on failure (with exponential backoff to avoid flooding Slack with messages).

## Deployment
Use the Ansible script in ansible/sherlock-wrapper.yml

This script has a single parameter for the Sherlock database host. To deploy with different databases for different wrapper hosts use the "-l" switch to limit ansible-playbook to the correct host(s), then edit the yml file and re-run for the other host(s). If the wrapper and database are on the same host then this is not necessary as the database host can simply be set to localhost.

## Build
The Ansible script deploys a Docker image. To build a new image use the Dockerfile then tag the image, push it to a repository and then edit the Ansible script to reference it.

At present the version string in wrapper.py and the version number of the docker image should match, e.g. image gpfrancis/sherlock-wrapper:0.5.16 contains wrapper.py version 0.5.16.
